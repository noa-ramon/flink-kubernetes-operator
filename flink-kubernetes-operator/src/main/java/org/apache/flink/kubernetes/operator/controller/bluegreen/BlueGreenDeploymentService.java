/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.controller.bluegreen;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDiffType;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.EventUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.deleteFlinkDeployment;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.deployCluster;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.isFlinkDeploymentReady;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.suspendFlinkDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.fetchSavepointInfo;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getCutoverDrainConfirmationWindow;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getCutoverDrainTimeout;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getReconciliationReschedInterval;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getSpecDiff;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.hasSpecChanged;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.instantStrToMillis;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.isSavepointRequired;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.isWaitForSourceDrainEnabled;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.millisToInstantStr;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.prepareFlinkDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.setLastReconciledSpec;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.triggerSavepoint;

/** Consolidated service for all Blue/Green deployment operations. */
public class BlueGreenDeploymentService {

    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenDeploymentService.class);
    private static final long RETRY_DELAY_MS = 500;
    private static final Duration CUTOVER_EVENT_INTERVAL = Duration.ofMinutes(1);

    // Every time comparison in this class goes through this clock so tests can drive the
    // deletion delay, the confirmation window and the drain deadline deterministically
    // instead of sleeping against the wall clock.
    private Clock clock = Clock.systemUTC();

    @VisibleForTesting
    public void setClock(Clock clock) {
        this.clock = clock;
    }

    /**
     * Per-resource start of the current run of zero-backlog readings, keyed to the job incarnation
     * that produced them. Operator-local by design: losing it on an operator restart only means the
     * window is measured again - a longer hold, never a wrong cutover.
     */
    private final Map<String, DrainConfirmation> drainConfirmations = new ConcurrentHashMap<>();

    private static String confirmationKey(BlueGreenContext context) {
        return context.getBgDeployment().getMetadata().getNamespace()
                + "/"
                + context.getDeploymentName();
    }

    private static final class DrainConfirmation {
        final long jobRunningTimestamp;
        final long sourcesEpoch;
        final Set<JobVertexID> reportingSources;
        final long firstZeroTimestamp;

        DrainConfirmation(
                long jobRunningTimestamp,
                long sourcesEpoch,
                Set<JobVertexID> reportingSources,
                long firstZeroTimestamp) {
            this.jobRunningTimestamp = jobRunningTimestamp;
            this.sourcesEpoch = sourcesEpoch;
            this.reportingSources = reportingSources;
            this.firstZeroTimestamp = firstZeroTimestamp;
        }
    }

    // ==================== Deployment Initiation Methods ====================

    /**
     * Initiates a new Blue/Green deployment.
     *
     * @param context the transition context
     * @param nextBlueGreenDeploymentType the type of deployment to create
     * @param nextState the next state to transition to
     * @param lastCheckpoint the checkpoint to restore from (can be null)
     * @param isFirstDeployment whether this is the first deployment
     * @return UpdateControl for the deployment
     */
    public UpdateControl<FlinkBlueGreenDeployment> initiateDeployment(
            BlueGreenContext context,
            BlueGreenDeploymentType nextBlueGreenDeploymentType,
            FlinkBlueGreenDeploymentState nextState,
            Savepoint lastCheckpoint,
            boolean isFirstDeployment) {
        ObjectMeta bgMeta = context.getBgDeployment().getMetadata();

        FlinkDeployment flinkDeployment =
                prepareFlinkDeployment(
                        context,
                        nextBlueGreenDeploymentType,
                        lastCheckpoint,
                        isFirstDeployment,
                        bgMeta);

        deployCluster(context, flinkDeployment);

        BlueGreenUtils.setAbortTimestamp(context);

        return patchStatusUpdateControl(context, nextState, JobStatus.RECONCILING, null)
                .rescheduleAfter(BlueGreenUtils.getReconciliationReschedInterval(context));
    }

    /**
     * Checks if a full transition can be initiated and initiates it if conditions are met.
     *
     * @param context the transition context
     * @param currentBlueGreenDeploymentType the current deployment type
     * @return UpdateControl for the deployment
     */
    public UpdateControl<FlinkBlueGreenDeployment> checkAndInitiateDeployment(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {

        BlueGreenDiffType specDiff = getSpecDiff(context);

        if (specDiff == BlueGreenDiffType.IGNORE
                && context.getDeploymentStatus().getSavepointTriggerId() != null) {
            // The savepoint trigger id is a baton: SAVEPOINTING stores it and returns to ACTIVE,
            // where the still-unreconciled spec diff re-enters this method and consumes it. If the
            // spec reverted in between (an external controller undoing the change), the diff reads
            // IGNORE, the baton is never consumed and it outlives the JobManager's completed
            // operation cache - so the next real transition, possibly days later, fetches a
            // trigger the JobManager no longer knows and fails before it starts. No pending diff
            // means any stored trigger id is stale: drop it with the other transition markers.
            LOG.info(
                    "Clearing stale savepoint trigger id '{}' for '{}': the transition it belonged"
                            + " to was reverted before the savepoint was consumed.",
                    context.getDeploymentStatus().getSavepointTriggerId(),
                    context.getDeploymentName());
            resetTransitionMarkers(context.getDeploymentStatus());
            return patchStatusUpdateControl(context, null, null, null)
                    .rescheduleAfter(getReconciliationReschedInterval(context));
        }

        if (specDiff != BlueGreenDiffType.IGNORE) {
            FlinkDeployment currentFlinkDeployment =
                    context.getDeploymentByType(currentBlueGreenDeploymentType);

            if (specDiff == BlueGreenDiffType.SUSPEND && currentFlinkDeployment != null) {
                setLastReconciledSpec(context);
                LOG.info(
                        "In-place suspension for '{}'",
                        currentFlinkDeployment.getMetadata().getName());
                return patchFlinkDeployment(context, currentBlueGreenDeploymentType);
            }

            if (specDiff == BlueGreenDiffType.RESUME && currentFlinkDeployment != null) {
                setLastReconciledSpec(context);
                LOG.info(
                        "In-place resume for '{}'", currentFlinkDeployment.getMetadata().getName());
                return patchFlinkDeployment(context, currentBlueGreenDeploymentType);
            }

            // Check if child is currently suspended - if so, just patch specs without restart
            if (isChildSuspended(currentFlinkDeployment)) {
                setLastReconciledSpec(context);
                LOG.info(
                        "Spec change while suspended for '{}'",
                        currentFlinkDeployment.getMetadata().getName());
                return patchFlinkDeployment(context, currentBlueGreenDeploymentType);
            }

            if (currentFlinkDeployment != null && isFlinkDeploymentReady(currentFlinkDeployment)) {
                if (specDiff == BlueGreenDiffType.TRANSITION) {
                    boolean savepointTriggered = false;
                    try {
                        savepointTriggered = handleSavepoint(context, currentFlinkDeployment);
                    } catch (Exception e) {
                        var error = "Could not trigger Savepoint. Details: " + e.getMessage();
                        return markDeploymentFailing(context, error);
                    }

                    if (savepointTriggered) {
                        // Spec is intentionally not marked as reconciled here to allow
                        // reprocessing the TRANSITION once savepoint creation completes
                        var savepointingState =
                                calculateSavepointingState(currentBlueGreenDeploymentType);
                        return patchStatusUpdateControl(context, savepointingState, null, null)
                                .rescheduleAfter(getReconciliationReschedInterval(context));
                    }

                    setLastReconciledSpec(context);
                    try {
                        return startTransition(
                                context, currentBlueGreenDeploymentType, currentFlinkDeployment);
                    } catch (Exception e) {
                        var error = "Could not start Transition. Details: " + e.getMessage();
                        context.getDeploymentStatus().setSavepointTriggerId(null);
                        return markDeploymentFailing(context, error);
                    }

                } else if (specDiff == BlueGreenDiffType.SAVEPOINT_REDEPLOY) {
                    // Savepoint redeploy: skip taking a new savepoint, use initialSavepointPath
                    var jobSpec =
                            context.getBgDeployment().getSpec().getTemplate().getSpec().getJob();
                    LOG.info(
                            "Savepoint redeploy triggered for '{}', using initialSavepointPath: {}",
                            context.getBgDeployment().getMetadata().getName(),
                            Objects.toString(jobSpec.getInitialSavepointPath(), "<none>"));
                    setLastReconciledSpec(context);
                    try {
                        return startSavepointRedeployTransition(
                                context, currentBlueGreenDeploymentType);
                    } catch (Exception e) {
                        var error =
                                "Could not start Savepoint Redeploy Transition. Details: "
                                        + e.getMessage();
                        return markDeploymentFailing(context, error);
                    }
                } else {
                    setLastReconciledSpec(context);
                    LOG.info(
                            "Patching FlinkDeployment '{}' during checkAndInitiateDeployment",
                            currentFlinkDeployment.getMetadata().getName());
                    return patchFlinkDeployment(context, currentBlueGreenDeploymentType);
                }
            } else {
                if (context.getDeploymentStatus().getJobStatus().getState() != JobStatus.FAILING) {
                    setLastReconciledSpec(context);
                    var childName =
                            currentFlinkDeployment != null
                                    ? currentFlinkDeployment.getMetadata().getName()
                                    : "null";
                    var error =
                            String.format(
                                    "Transition to %s not possible, current Flink Deployment '%s' is not READY. FAILING '%s'",
                                    calculateTransition(currentBlueGreenDeploymentType)
                                            .nextBlueGreenDeploymentType,
                                    childName,
                                    context.getBgDeployment().getMetadata().getName());
                    return markDeploymentFailing(context, error);
                }
            }
        }

        return UpdateControl.noUpdate();
    }

    private boolean isChildSuspended(FlinkDeployment deployment) {
        if (deployment == null || deployment.getSpec() == null) {
            return false;
        }
        var job = deployment.getSpec().getJob();
        return job != null
                && job.getState()
                        == org.apache.flink.kubernetes.operator.api.spec.JobState.SUSPENDED;
    }

    private UpdateControl<FlinkBlueGreenDeployment> patchFlinkDeployment(
            BlueGreenContext context, BlueGreenDeploymentType blueGreenDeploymentTypeToPatch) {
        return patchFlinkDeployment(context, blueGreenDeploymentTypeToPatch, true);
    }

    private UpdateControl<FlinkBlueGreenDeployment> patchFlinkDeployment(
            BlueGreenContext context,
            BlueGreenDeploymentType blueGreenDeploymentTypeToPatch,
            boolean carryOverSavepointInPatch) {

        String childDeploymentName =
                context.getBgDeployment().getMetadata().getName()
                        + "-"
                        + blueGreenDeploymentTypeToPatch.toString().toLowerCase();

        // We want to patch, therefore the transition should point to the existing deployment
        // details
        var patchingState = calculatePatchingState(blueGreenDeploymentTypeToPatch);

        // If we're not transitioning between deployments, mark as a single deployment to have it
        // not wait for synchronization
        var isFirstDeployment = context.getDeployments().getNumberOfDeployments() != 2;

        // TODO: if the resource failed right after being deployed with an initialSavepointPath,
        //  will it be used by this patching? otherwise this is unnecessary, keep lastSavepoint =
        // null.
        Savepoint lastSavepoint =
                carryOverSavepointInPatch
                        ? carryOverSavepoint(
                                context, blueGreenDeploymentTypeToPatch, childDeploymentName)
                        : null;

        return initiateDeployment(
                context,
                blueGreenDeploymentTypeToPatch,
                patchingState,
                lastSavepoint,
                isFirstDeployment);
    }

    @Nullable
    private static Savepoint carryOverSavepoint(
            BlueGreenContext context,
            BlueGreenDeploymentType blueGreenDeploymentTypeToPatch,
            String childDeploymentName) {
        var deploymentToPatch = context.getDeploymentByType(blueGreenDeploymentTypeToPatch);
        var initialSavepointPath = deploymentToPatch.getSpec().getJob().getInitialSavepointPath();

        if (initialSavepointPath == null || initialSavepointPath.isEmpty()) {
            initialSavepointPath =
                    deploymentToPatch.getStatus().getJobStatus().getUpgradeSavepointPath();
        }

        Savepoint lastSavepoint = null;
        if (initialSavepointPath != null && !initialSavepointPath.isEmpty()) {
            var ctx =
                    context.getCtxFactory()
                            .getResourceContext(deploymentToPatch, context.getJosdkContext());

            lastSavepoint = getSavepointObject(ctx, initialSavepointPath);

            LOG.info(
                    "Patching FlinkDeployment '{}', carrying over Savepoint at: '{}'",
                    childDeploymentName,
                    initialSavepointPath);
        } else {
            LOG.info("Patching FlinkDeployment '{}'", childDeploymentName);
        }

        return lastSavepoint;
    }

    private UpdateControl<FlinkBlueGreenDeployment> startTransition(
            BlueGreenContext context,
            BlueGreenDeploymentType currentBlueGreenDeploymentType,
            FlinkDeployment currentFlinkDeployment) {
        DeploymentTransition transition = calculateTransition(currentBlueGreenDeploymentType);

        Savepoint lastCheckpoint = configureInitialSavepoint(context, currentFlinkDeployment);

        return initiateDeployment(
                context,
                transition.nextBlueGreenDeploymentType,
                transition.nextState,
                lastCheckpoint,
                false);
    }

    /**
     * Starts a transition for savepoint redeploy scenario. Unlike normal transitions, this does not
     * take a new savepoint - it uses the initialSavepointPath specified in the spec.
     *
     * @param context the transition context
     * @param currentBlueGreenDeploymentType the current deployment type
     * @return UpdateControl for the deployment
     */
    private UpdateControl<FlinkBlueGreenDeployment> startSavepointRedeployTransition(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {
        DeploymentTransition transition = calculateTransition(currentBlueGreenDeploymentType);

        return initiateDeployment(
                context,
                transition.nextBlueGreenDeploymentType,
                transition.nextState,
                null, // Use initialSavepointPath from spec
                false);
    }

    private DeploymentTransition calculateTransition(BlueGreenDeploymentType currentType) {
        if (BlueGreenDeploymentType.BLUE == currentType) {
            return new DeploymentTransition(
                    BlueGreenDeploymentType.GREEN,
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN);
        } else {
            return new DeploymentTransition(
                    BlueGreenDeploymentType.BLUE,
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE);
        }
    }

    private FlinkBlueGreenDeploymentState calculatePatchingState(
            BlueGreenDeploymentType currentType) {
        if (BlueGreenDeploymentType.BLUE == currentType) {
            return FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
        } else {
            return FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
        }
    }

    // ==================== Savepointing Methods ====================

    public boolean monitorSavepoint(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {

        FlinkResourceContext<FlinkDeployment> ctx =
                context.getCtxFactory()
                        .getResourceContext(
                                context.getDeploymentByType(currentBlueGreenDeploymentType),
                                context.getJosdkContext());

        String savepointTriggerId = context.getDeploymentStatus().getSavepointTriggerId();
        var savepointFetchResult = fetchSavepointInfo(ctx, savepointTriggerId);

        return !savepointFetchResult.isPending();
    }

    private Savepoint configureInitialSavepoint(
            BlueGreenContext context, FlinkDeployment currentFlinkDeployment) {
        // Create savepoint for all upgrade modes except STATELESS
        // (originally only SAVEPOINT mode required savepoints)
        if (isSavepointRequired(context)) {
            FlinkResourceContext<FlinkDeployment> ctx =
                    context.getCtxFactory()
                            .getResourceContext(currentFlinkDeployment, context.getJosdkContext());

            String triggerId = context.getDeploymentStatus().getSavepointTriggerId();
            var savepointFetchResult = fetchSavepointInfo(ctx, triggerId);

            if (savepointFetchResult.getError() != null
                    && !savepointFetchResult.getError().isEmpty()) {
                throw new RuntimeException(
                        String.format(
                                "Could not fetch savepoint with triggerId: %s. Error: %s",
                                triggerId, savepointFetchResult.getError()));
            }

            return getSavepointObject(ctx, savepointFetchResult.getLocation());
        }

        // Currently not using last checkpoint recovery for LAST_STATE upgrade mode
        // This could be re-enabled in the future by uncommenting the logic below
        return null;

        //        if (!lookForCheckpoint(context)) {
        //            return null;
        //        }
        //
        //        return getLastCheckpoint(ctx);
    }

    @NotNull
    private static Savepoint getSavepointObject(
            FlinkResourceContext<FlinkDeployment> ctx, String savepointLocation) {
        org.apache.flink.core.execution.SavepointFormatType coreSavepointFormatType =
                ctx.getObserveConfig()
                        .get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);

        var savepointFormatType = SavepointFormatType.valueOf(coreSavepointFormatType.toString());

        return Savepoint.of(savepointLocation, SnapshotTriggerType.MANUAL, savepointFormatType);
    }

    private boolean handleSavepoint(
            BlueGreenContext context, FlinkDeployment currentFlinkDeployment) throws Exception {

        if (!isSavepointRequired(context)) {
            return false;
        }

        FlinkResourceContext<FlinkDeployment> ctx =
                context.getCtxFactory()
                        .getResourceContext(currentFlinkDeployment, context.getJosdkContext());

        String savepointTriggerId = context.getDeploymentStatus().getSavepointTriggerId();

        if (savepointTriggerId == null || savepointTriggerId.isEmpty()) {
            String triggerId = triggerSavepoint(ctx);
            LOG.info("Savepoint requested (triggerId: {}", triggerId);
            context.getDeploymentStatus().setSavepointTriggerId(triggerId);
            return true;
        }

        LOG.info("Savepoint previously requested (triggerId: {})", savepointTriggerId);
        return false;
    }

    private FlinkBlueGreenDeploymentState calculateSavepointingState(
            BlueGreenDeploymentType currentType) {
        if (BlueGreenDeploymentType.BLUE == currentType) {
            return FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE;
        } else {
            return FlinkBlueGreenDeploymentState.SAVEPOINTING_GREEN;
        }
    }

    // ==================== Transition Monitoring Methods ====================

    /**
     * Monitors an ongoing Blue/Green deployment transition.
     *
     * @param context the transition context
     * @param currentBlueGreenDeploymentType the current deployment type being transitioned from
     * @return UpdateControl for the transition
     */
    public UpdateControl<FlinkBlueGreenDeployment> monitorTransition(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {

        var updateControl =
                handleSpecChangesDuringTransition(context, currentBlueGreenDeploymentType);

        if (updateControl != null) {
            return updateControl;
        }

        TransitionState transitionState =
                determineTransitionState(context, currentBlueGreenDeploymentType);

        if (isChildSuspended(transitionState.nextDeployment)) {
            if (transitionState.nextDeployment.getStatus().getLifecycleState()
                    == ResourceLifecycleState.SUSPENDED) {
                return finalizeSuspendedDeployment(context, transitionState.nextState);
            } else {
                return shouldWeAbort(
                        context, transitionState.nextDeployment, transitionState.nextState);
            }
        }

        if (isFlinkDeploymentReady(transitionState.nextDeployment)) {
            return shouldWeDelete(
                    context,
                    transitionState.currentDeployment,
                    transitionState.nextDeployment,
                    transitionState.nextState);
        } else {
            return shouldWeAbort(
                    context, transitionState.nextDeployment, transitionState.nextState);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> finalizeSuspendedDeployment(
            BlueGreenContext context, FlinkBlueGreenDeploymentState nextState) {

        LOG.info(
                "Finalizing suspended deployment '{}' to {} state",
                context.getDeploymentName(),
                nextState);

        resetTransitionMarkers(context.getDeploymentStatus());
        drainConfirmations.remove(confirmationKey(context));

        return patchStatusUpdateControl(context, nextState, JobStatus.SUSPENDED, null)
                .rescheduleAfter(0);
    }

    private UpdateControl<FlinkBlueGreenDeployment> handleSpecChangesDuringTransition(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {
        if (hasSpecChanged(context)) {
            BlueGreenDiffType diffType = getSpecDiff(context);

            // Block SUSPEND during transition - wait for transition to complete first
            if (diffType == BlueGreenDiffType.SUSPEND) {
                LOG.info(
                        "Suspend requested during transition for '{}'. "
                                + "Waiting for transition to complete before processing suspend.",
                        context.getBgDeployment().getMetadata().getName());
                return null;
            }

            if (diffType != BlueGreenDiffType.IGNORE) {
                setLastReconciledSpec(context);
                var oppositeDeploymentType =
                        context.getOppositeDeploymentType(currentBlueGreenDeploymentType);
                LOG.info(
                        "Patching FlinkDeployment '{}' during handleSpecChangesDuringTransition",
                        context.getDeploymentByType(oppositeDeploymentType)
                                .getMetadata()
                                .getName());
                return patchFlinkDeployment(
                        context,
                        oppositeDeploymentType,
                        diffType != BlueGreenDiffType.SAVEPOINT_REDEPLOY);
            }
        }

        return null;
    }

    private TransitionState determineTransitionState(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {
        TransitionState transitionState;

        if (BlueGreenDeploymentType.BLUE == currentBlueGreenDeploymentType) {
            transitionState =
                    new TransitionState(
                            context.getBlueDeployment(), // currentDeployment
                            context.getGreenDeployment(), // nextDeployment
                            FlinkBlueGreenDeploymentState.ACTIVE_GREEN); // next State
        } else {
            transitionState =
                    new TransitionState(
                            context.getGreenDeployment(), // currentDeployment
                            context.getBlueDeployment(), // nextDeployment
                            FlinkBlueGreenDeploymentState.ACTIVE_BLUE); // next State
        }

        Preconditions.checkNotNull(
                transitionState.nextDeployment,
                "Target Dependent Deployment resource not found. Blue/Green deployment name: "
                        + context.getDeploymentName()
                        + ", current deployment type: "
                        + currentBlueGreenDeploymentType);

        return transitionState;
    }

    // ==================== Deployment Deletion Methods ====================

    private UpdateControl<FlinkBlueGreenDeployment> shouldWeDelete(
            BlueGreenContext context,
            FlinkDeployment currentDeployment,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState) {

        var deploymentStatus = context.getDeploymentStatus();

        if (currentDeployment == null) {
            deploymentStatus.setDeploymentReadyTimestamp(Instant.now(clock).toString());
            return finalizeBlueGreenDeployment(context, nextState);
        }

        long deploymentDeletionDelayMs = BlueGreenUtils.getDeploymentDeletionDelay(context);
        long deploymentReadyTimestamp =
                instantStrToMillis(deploymentStatus.getDeploymentReadyTimestamp());

        if (deploymentReadyTimestamp == 0) {
            LOG.info(
                    "FlinkDeployment '{}' marked ready, rescheduling reconciliation in {} seconds.",
                    nextDeployment.getMetadata().getName(),
                    deploymentDeletionDelayMs / 1000);

            deploymentStatus.setDeploymentReadyTimestamp(Instant.now(clock).toString());
            return patchStatusUpdateControl(context, null, null, null)
                    .rescheduleAfter(deploymentDeletionDelayMs);
        }

        long deletionTimestamp = deploymentReadyTimestamp + deploymentDeletionDelayMs;

        if (deletionTimestamp < clock.millis()) {
            if (isWaitForSourceDrainEnabled(context)) {
                return handleSourceDrain(
                        context,
                        currentDeployment,
                        nextDeployment,
                        nextState,
                        deploymentReadyTimestamp);
            }
            return deleteDeployment(currentDeployment, context, nextState);
        } else {
            return waitBeforeDeleting(currentDeployment, deletionTimestamp);
        }
    }

    /**
     * Keeps the previous deployment alive until the new deployment has drained its source backlog.
     *
     * <p>A deployment restored from the transition savepoint is ready while it still replays the
     * records produced during its own startup. For event time pipelines with a bounded allowed
     * lateness, deleting the previous deployment during that catch-up leaves those records without
     * a producer, so the drain is a correctness condition rather than a freshness one.
     *
     * <p>If the backlog is not drained within the configured timeout the transition is aborted: the
     * new deployment is suspended and the previous one stays active, consistent with how a
     * deployment that never becomes ready is handled.
     */
    private UpdateControl<FlinkBlueGreenDeployment> handleSourceDrain(
            BlueGreenContext context,
            FlinkDeployment currentDeployment,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState,
            long deploymentReadyTimestamp) {

        String nextDeploymentName = nextDeployment.getMetadata().getName();
        long reschedInterval = getReconciliationReschedInterval(context);
        String notDrainedReason = null;
        FlinkService.SourceBacklogSnapshot backlog = null;

        try {
            backlog = getSourcePendingRecords(context, nextDeployment);
        } catch (Exception e) {
            // Treat an unreachable job as not drained: the previous deployment keeps producing
            // until the metric can be read again or the drain timeout aborts the transition.
            LOG.warn(
                    "Could not read pending records for FlinkDeployment '{}', will retry.",
                    nextDeploymentName,
                    e);
            notDrainedReason = "the pending records metric could not be read";
            // The window claims the backlog read as CONTINUOUSLY empty; a gap in which it could
            // not be read at all breaks that claim, so the window starts over once reads resume.
            drainConfirmations.remove(confirmationKey(context));
        }

        // A single reading is never evidence of a drain: sources initialise pendingRecords lazily
        // and a restart wipes the gauge, so a lone zero - or silence - can be an artifact of the
        // metric rather than of the backlog. The cutover requires the backlog to read as
        // continuously empty for a confirmation window, measured from the first zero reading of
        // the current incarnation of the job (same RUNNING timestamp, same source vertex start
        // times) and the same set of reporting sources - never from wall time alone. A non-zero
        // reading, silence, a restart or a change in who reports restarts the window. A job whose
        // sources never report holds until the drain timeout aborts the transition: FLIP-33 marks
        // the metric optional, so silence is a normal state and never evidence of a drain.
        long confirmationWindow = getCutoverDrainConfirmationWindow(context);
        String confirmationKey = confirmationKey(context);
        long zeroConfirmedFor = 0;

        if (notDrainedReason == null) {
            if (backlog.getPendingRecords().isEmpty()) {
                drainConfirmations.remove(confirmationKey);
                notDrainedReason = "no source reports the pending records metric";
            } else if (backlog.getPendingRecords().getAsLong() > 0) {
                drainConfirmations.remove(confirmationKey);
                notDrainedReason =
                        String.format(
                                "it has %d pending records",
                                backlog.getPendingRecords().getAsLong());
            } else if (backlog.getJobRunningTimestamp() == 0) {
                // No RUNNING timestamp means the job details were taken while the job was not
                // observably running (e.g. mid-restart) - a zero read then is a stale aggregate,
                // not evidence, and it must not anchor or extend the window.
                drainConfirmations.remove(confirmationKey);
                notDrainedReason = "the job is not observably running";
            } else {
                long now = clock.millis();
                DrainConfirmation previous = drainConfirmations.get(confirmationKey);
                boolean sameIncarnation =
                        previous != null
                                && previous.jobRunningTimestamp == backlog.getJobRunningTimestamp()
                                && previous.sourcesEpoch == backlog.getSourcesEpoch()
                                && previous.reportingSources.equals(backlog.getReportingSources());
                long firstZeroTimestamp = sameIncarnation ? previous.firstZeroTimestamp : now;
                drainConfirmations.put(
                        confirmationKey,
                        new DrainConfirmation(
                                backlog.getJobRunningTimestamp(),
                                backlog.getSourcesEpoch(),
                                backlog.getReportingSources(),
                                firstZeroTimestamp));
                zeroConfirmedFor = now - firstZeroTimestamp;
                if (zeroConfirmedFor < confirmationWindow) {
                    notDrainedReason =
                            String.format(
                                    "the empty backlog has been confirmed for %d of the required"
                                            + " %d seconds",
                                    zeroConfirmedFor / 1000, confirmationWindow / 1000);
                }
            }
        }

        long heldFor = clock.millis() - deploymentReadyTimestamp;
        // The gate is only entered after the deployment deletion delay has elapsed, so the
        // deadline is anchored there rather than at readiness: anchored at readiness, a delay
        // longer than the drain timeout would expire the deadline before the first backlog
        // reading is ever taken and abort a healthy transition on entry.
        long gateOpenTimestamp =
                deploymentReadyTimestamp + BlueGreenUtils.getDeploymentDeletionDelay(context);
        long drainDeadline = gateOpenTimestamp + getCutoverDrainTimeout(context);
        boolean pastDeadline = drainDeadline < clock.millis();

        // One audit record per evaluation, so the drain can be reconstructed from the operator log
        // alone: a gate that decides when the previous deployment stops producing has to leave
        // evidence of every observation it acted on, not only of the decisions that changed state.
        LOG.info(
                "Cutover gate for '{}': pendingRecords={}, sources={}, zeroFor={}/{} s,"
                        + " heldFor={} s, drainTimeoutIn={} s, verdict={}{}",
                nextDeploymentName,
                backlog == null
                        ? "unknown"
                        : backlog.getPendingRecords().isPresent()
                                ? String.valueOf(backlog.getPendingRecords().getAsLong())
                                : "unreported",
                backlog == null
                        ? "?/?"
                        : backlog.getReportingSources().size() + "/" + backlog.getRunningSources(),
                zeroConfirmedFor / 1000,
                confirmationWindow / 1000,
                heldFor / 1000,
                Math.max(0, drainDeadline - clock.millis()) / 1000,
                pastDeadline ? "ABORT" : (notDrainedReason == null ? "CUTOVER" : "HOLD"),
                notDrainedReason == null ? "" : " (" + notDrainedReason + ")");

        // The deadline is evaluated before the backlog, so an expired hold can only ever end in an
        // abort. Checking it second made the timeout advisory: a zero arriving at expiry won it,
        // and a lost status update re-opened the gate for the next reconcile to promote on. A
        // drain that finishes microseconds after the deadline is failed deliberately - a deadline
        // that a late reading can overrule is not a deadline.
        if (pastDeadline) {
            String aborted =
                    String.format(
                            "FlinkDeployment '%s' did not drain its source backlog within the drain"
                                    + " timeout (%s), aborting the transition and keeping '%s'.",
                            nextDeploymentName,
                            notDrainedReason == null
                                    ? "the deadline expired as it reported a drained backlog"
                                    : notDrainedReason,
                            currentDeployment.getMetadata().getName());
            LOG.error(aborted);
            emitCutoverEvent(context, EventRecorder.Type.Warning, "CutoverAborted", aborted);
            return abortDeployment(context, nextDeployment, nextState, nextDeploymentName);
        }

        if (notDrainedReason == null) {
            String drained =
                    String.format(
                            "FlinkDeployment '%s' drained its source backlog (0 pending, confirmed"
                                    + " for %d s by %d of %d sources, held for %d s), proceeding"
                                    + " with the cutover.",
                            nextDeploymentName,
                            zeroConfirmedFor / 1000,
                            backlog.getReportingSources().size(),
                            backlog.getRunningSources(),
                            heldFor / 1000);
            emitCutoverEvent(context, EventRecorder.Type.Normal, "CutoverDrained", drained);
            return deleteDeployment(currentDeployment, context, nextState);
        }

        String held =
                String.format(
                        "Not cutting over to FlinkDeployment '%s' yet because %s; keeping '%s'"
                                + " (held for %d s).",
                        nextDeploymentName,
                        notDrainedReason,
                        currentDeployment.getMetadata().getName(),
                        (clock.millis() - deploymentReadyTimestamp) / 1000);
        emitCutoverEvent(context, EventRecorder.Type.Normal, "CutoverHeld", held);

        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(reschedInterval);
    }

    /**
     * Surfaces a cutover decision as a Kubernetes event on the Blue/Green resource, so that a hold,
     * a drain or an abort is visible through {@code kubectl describe} and any event-based tooling
     * rather than only in the operator log. Repeats of the same message are collapsed over a short
     * interval.
     */
    private void emitCutoverEvent(
            BlueGreenContext context, EventRecorder.Type type, String reason, String message) {
        try {
            EventUtils.createOrUpdateEventWithInterval(
                    context.getJosdkContext().getClient(),
                    context.getBgDeployment(),
                    type,
                    reason,
                    message,
                    EventRecorder.Component.Operator,
                    event -> {},
                    reason,
                    CUTOVER_EVENT_INTERVAL);
        } catch (Exception e) {
            // Visibility must never break the reconciliation it is reporting on
            LOG.warn("Could not emit cutover event for '{}'", context.getDeploymentName(), e);
        }
    }

    private FlinkService.SourceBacklogSnapshot getSourcePendingRecords(
            BlueGreenContext context, FlinkDeployment deployment) throws Exception {
        FlinkResourceContext<FlinkDeployment> ctx =
                context.getCtxFactory().getResourceContext(deployment, context.getJosdkContext());
        return ctx.getFlinkService()
                .getSourcePendingRecords(
                        ctx.getObserveConfig(), deployment.getStatus().getJobStatus().getJobId());
    }

    private UpdateControl<FlinkBlueGreenDeployment> waitBeforeDeleting(
            FlinkDeployment currentDeployment, long deletionTimestamp) {

        long delay = deletionTimestamp - clock.millis();
        LOG.info(
                "Awaiting deletion delay for FlinkDeployment '{}', rescheduling reconciliation in {} seconds.",
                currentDeployment.getMetadata().getName(),
                delay / 1000);

        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(delay);
    }

    private UpdateControl<FlinkBlueGreenDeployment> deleteDeployment(
            FlinkDeployment currentDeployment,
            BlueGreenContext context,
            FlinkBlueGreenDeploymentState nextState) {

        boolean deleted = deleteFlinkDeployment(currentDeployment, context);

        if (!deleted) {
            LOG.info(
                    "FlinkDeployment '{}' not deleted, will retry",
                    currentDeployment.getMetadata().getName());
            return UpdateControl.<FlinkBlueGreenDeployment>noUpdate()
                    .rescheduleAfter(RETRY_DELAY_MS);
        } else {
            LOG.info("FlinkDeployment '{}' deleted!", currentDeployment.getMetadata().getName());
            return finalizeBlueGreenDeployment(context, nextState);
        }
    }

    // ==================== Abort and Retry Methods ====================

    private UpdateControl<FlinkBlueGreenDeployment> shouldWeAbort(
            BlueGreenContext context,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState) {

        String deploymentName = nextDeployment.getMetadata().getName();
        long abortTimestamp = instantStrToMillis(context.getDeploymentStatus().getAbortTimestamp());

        if (abortTimestamp == 0) {
            throw new IllegalStateException("Unexpected abortTimestamp == 0");
        }

        if (abortTimestamp < clock.millis()) {
            return abortDeployment(context, nextDeployment, nextState, deploymentName);
        } else {
            return retryDeployment(context, deploymentName);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> retryDeployment(
            BlueGreenContext context, String deploymentName) {

        long delay = getReconciliationReschedInterval(context);

        LOG.info(
                "FlinkDeployment '{}' not ready yet, retrying in {} seconds.",
                deploymentName,
                delay / 1000);

        return patchStatusUpdateControl(context, null, null, null).rescheduleAfter(delay);
    }

    private UpdateControl<FlinkBlueGreenDeployment> abortDeployment(
            BlueGreenContext context,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState,
            String deploymentName) {

        suspendFlinkDeployment(context, nextDeployment);

        FlinkBlueGreenDeploymentState previousState =
                getPreviousState(nextState, context.getDeployments());
        context.getDeploymentStatus().setBlueGreenState(previousState);
        resetTransitionMarkers(context.getDeploymentStatus());
        drainConfirmations.remove(confirmationKey(context));

        var error =
                String.format(
                        "Aborting deployment '%s', rolling B/G deployment back to %s",
                        deploymentName, previousState);
        return markDeploymentFailing(context, error);
    }

    @NotNull
    private static UpdateControl<FlinkBlueGreenDeployment> markDeploymentFailing(
            BlueGreenContext context, String error) {
        LOG.error(error);
        return patchStatusUpdateControl(context, null, JobStatus.FAILING, error);
    }

    private static FlinkBlueGreenDeploymentState getPreviousState(
            FlinkBlueGreenDeploymentState nextState, FlinkBlueGreenDeployments deployments) {
        FlinkBlueGreenDeploymentState previousState;
        if (deployments.getNumberOfDeployments() == 1) {
            previousState = FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
        } else if (deployments.getNumberOfDeployments() == 2) {
            previousState =
                    nextState == FlinkBlueGreenDeploymentState.ACTIVE_BLUE
                            ? FlinkBlueGreenDeploymentState.ACTIVE_GREEN
                            : FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
        } else {
            throw new IllegalStateException("No blue/green FlinkDeployments found!");
        }
        return previousState;
    }

    // ==================== Finalization Methods ====================

    /**
     * Finalizes a Blue/Green deployment transition.
     *
     * @param context the transition context
     * @param nextState the next state to transition to
     * @return UpdateControl for finalization
     */
    public UpdateControl<FlinkBlueGreenDeployment> finalizeBlueGreenDeployment(
            BlueGreenContext context, FlinkBlueGreenDeploymentState nextState) {

        LOG.info("Finalizing deployment '{}' to {} state", context.getDeploymentName(), nextState);

        resetTransitionMarkers(context.getDeploymentStatus());
        drainConfirmations.remove(confirmationKey(context));

        updateBlueGreenIngress(context, nextState);

        // Finalize status and reschedule immediately so any pending spec changes
        // (e.g., suspend requested during transition) are picked up on next reconcile
        return patchStatusUpdateControl(context, nextState, JobStatus.RUNNING, null)
                .rescheduleAfter(0);
    }

    /**
     * Reconciles ingress for the active deployment in ACTIVE states. This handles ingress spec
     * changes that occur while the deployment is stable (not transitioning).
     *
     * @param context the Blue/Green context
     * @param activeDeploymentType which deployment (BLUE or GREEN) is currently active
     */
    public void reconcileIngressForActiveDeployment(
            BlueGreenContext context, BlueGreenDeploymentType activeDeploymentType) {
        FlinkDeployment activeDeployment = context.getDeploymentByType(activeDeploymentType);
        if (activeDeployment == null) {
            return;
        }

        var flinkResourceContext =
                context.getCtxFactory()
                        .getResourceContext(activeDeployment, context.getJosdkContext());

        if (!flinkResourceContext.getOperatorConfig().isManageIngress()) {
            return;
        }

        IngressUtils.reconcileBlueGreenIngress(
                context,
                true,
                activeDeployment,
                flinkResourceContext.getDeployConfig(activeDeployment.getSpec()),
                context.getJosdkContext());

        LOG.info(
                "Successfully reconciled ingress for active deployment: {}",
                activeDeployment.getMetadata().getName());
    }

    /**
     * Updates the ingress for Blue/Green deployment during transitions, pointing to the newly
     * active deployment.
     *
     * @param blueGreenContext the Blue/Green context
     * @param nextState which deployment (ACTIVE_BLUE or ACTIVE_GREEN) is becoming active
     */
    public void updateBlueGreenIngress(
            BlueGreenContext blueGreenContext, FlinkBlueGreenDeploymentState nextState) {
        FlinkDeployment activeDeployment;
        switch (nextState) {
            case ACTIVE_BLUE:
                activeDeployment = blueGreenContext.getBlueDeployment();
                break;
            case ACTIVE_GREEN:
                activeDeployment = blueGreenContext.getGreenDeployment();
                break;
            default:
                LOG.info("Skipping ingress reconciliation for non-active state: {}", nextState);
                return;
        }

        // Create a FlinkResourceContext for the active deployment to get proper config
        var flinkResourceContext =
                blueGreenContext
                        .getCtxFactory()
                        .getResourceContext(activeDeployment, blueGreenContext.getJosdkContext());

        IngressUtils.reconcileBlueGreenIngress(
                blueGreenContext,
                flinkResourceContext.getOperatorConfig().isManageIngress(),
                activeDeployment,
                flinkResourceContext.getDeployConfig(activeDeployment.getSpec()),
                blueGreenContext.getJosdkContext());
    }

    // ==================== Common Utility Methods ====================

    /**
     * Clears the markers that scope a transition, so the next one starts from a zero stopwatch.
     * Every path that ends a transition must call this: the deployment ready timestamp is the
     * origin of the cutover gate's hold clock, its settle window and its drain timeout, so a value
     * left behind by one transition silently pre-expires all three on the next.
     */
    private static void resetTransitionMarkers(FlinkBlueGreenDeploymentStatus status) {
        status.setDeploymentReadyTimestamp(millisToInstantStr(0));
        status.setAbortTimestamp(millisToInstantStr(0));
        status.setSavepointTriggerId(null);
    }

    public static UpdateControl<FlinkBlueGreenDeployment> patchStatusUpdateControl(
            BlueGreenContext context,
            FlinkBlueGreenDeploymentState deploymentState,
            JobStatus jobState,
            String error) {

        var deploymentStatus = context.getDeploymentStatus();
        var flinkBlueGreenDeployment = context.getBgDeployment();

        if (deploymentState != null) {
            deploymentStatus.setBlueGreenState(deploymentState);
        }

        if (jobState != null) {
            deploymentStatus.getJobStatus().setState(jobState);
        }

        if (jobState == JobStatus.FAILING) {
            deploymentStatus.setError(error);
        }

        if (jobState == JobStatus.RECONCILING || jobState == JobStatus.RUNNING) {
            deploymentStatus.setError(null);
        }

        deploymentStatus.setLastReconciledTimestamp(java.time.Instant.now().toString());
        flinkBlueGreenDeployment.setStatus(deploymentStatus);
        return UpdateControl.patchStatus(flinkBlueGreenDeployment);
    }

    // ==================== DTO/Result Classes ====================

    @Getter
    @AllArgsConstructor
    private static class DeploymentTransition {
        final BlueGreenDeploymentType nextBlueGreenDeploymentType;
        final FlinkBlueGreenDeploymentState nextState;
    }

    @Getter
    @AllArgsConstructor
    private static class TransitionState {
        final FlinkDeployment currentDeployment;
        final FlinkDeployment nextDeployment;
        final FlinkBlueGreenDeploymentState nextState;
    }
}
