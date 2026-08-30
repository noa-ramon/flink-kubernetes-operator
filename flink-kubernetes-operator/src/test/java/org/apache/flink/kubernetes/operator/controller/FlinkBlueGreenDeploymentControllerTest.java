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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.ABORT_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.CUTOVER_DRAIN_TIMEOUT;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.CUTOVER_WAIT_FOR_SOURCE_DRAIN;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.RECONCILIATION_RESCHEDULING_INTERVAL;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.SAMPLE_JAR;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_DEPLOYMENT_NAME;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_NAMESPACE;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.instantStrToMillis;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.millisToInstantStr;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link FlinkBlueGreenDeploymentController} tests. */
@EnableKubernetesMockClient(crud = true)
public class FlinkBlueGreenDeploymentControllerTest {

    public static final String SERVICE_ACCOUNT = "flink-operator";
    public static final String FLINK_VERSION = "latest";
    public static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    public static final String IMAGE_POLICY = "IfNotPresent";

    private static final String CUSTOM_CONFIG_FIELD = "custom-configuration-field";
    private static final int DEFAULT_DELETION_DELAY_VALUE = 500;
    private static final int ALT_DELETION_DELAY_VALUE = 1000;
    private static final int MINIMUM_ABORT_GRACE_PERIOD = 1000;
    private static final String TEST_CHECKPOINT_PATH = "/tmp/checkpoints";
    private static final String TEST_INITIAL_SAVEPOINT_PATH = "/tmp/savepoints";
    private static final String BLUE_CLUSTER_ID = TEST_DEPLOYMENT_NAME + "-blue";
    private static final String GREEN_CLUSTER_ID = TEST_DEPLOYMENT_NAME + "-green";
    private static final String REST_SVC_NAME_SUFFIX = "-rest";
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private TestingFlinkService flinkService;
    private Context<FlinkBlueGreenDeployment> context;
    private TestingFlinkBlueGreenDeploymentController testController;

    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService(kubernetesClient);
        context = flinkService.getContext();
        testController = new TestingFlinkBlueGreenDeploymentController(configManager, flinkService);
    }

    @ParameterizedTest
    @MethodSource("flinkVersionsAndSavepointPaths")
    public void verifyBasicDeployment(FlinkVersion flinkVersion, String initialSavepointPath)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        initialSavepointPath,
                        UpgradeMode.STATELESS);
        executeBasicDeployment(flinkVersion, blueGreenDeployment, true, initialSavepointPath);
    }

    @ParameterizedTest
    @MethodSource("flinkVersionsAndSavepointPathsAndUpgradeModes")
    public void verifyBasicTransition(
            FlinkVersion flinkVersion, String initialSavepointPath, UpgradeMode upgradeMode)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion, null, upgradeMode);
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // Simulate a change in the spec to trigger a Green deployment
        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(
                rs.deployment, customValue, ALT_DELETION_DELAY_VALUE, initialSavepointPath);

        var expectedSavepointPath = initialSavepointPath;

        if (upgradeMode != UpgradeMode.STATELESS) {
            // In this case there will ALWAYS be a savepoint generated with this value,
            // regardless of the initialSavepointPath
            expectedSavepointPath = "savepoint_1";
            rs = handleSavepoint(rs);
        }

        // Transitioning to the Green deployment

        testTransitionToGreen(rs, customValue, expectedSavepointPath);
    }

    @NotNull
    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult handleSavepoint(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs)
            throws Exception {

        var triggers = flinkService.getSavepointTriggers();
        triggers.clear();

        rs = reconcile(rs.deployment);

        // Simulating a pending savepoint
        triggers.put(rs.deployment.getStatus().getSavepointTriggerId(), false);

        // Should be in SAVEPOINTING_BLUE state first
        assertEquals(
                FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE,
                rs.reconciledStatus.getBlueGreenState());
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(rs.updateControl.getScheduleDelay().isPresent());

        // This next reconciliation should continue waiting on the pending savepoint
        rs = reconcile(rs.deployment);

        // NOTE: internally the above reconcile call invokes the fetchSavepointInfo on the trigger,
        // the TestFlinkService automatically sets it to "true" (completed)

        assertTrue(rs.updateControl.isNoUpdate());
        assertTrue(rs.updateControl.getScheduleDelay().isPresent());

        // Completing the savepoint
        triggers.put(rs.deployment.getStatus().getSavepointTriggerId(), true);

        // This next reconciliation should move on to the next state
        rs = reconcile(rs.deployment);

        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE, rs.reconciledStatus.getBlueGreenState());
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(rs.updateControl.getScheduleDelay().isPresent());
        return rs;
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySavepointRedeployNonceTriggersTransitionWithInitialSavepointPath(
            FlinkVersion flinkVersion) throws Exception {
        // Start with SAVEPOINT upgrade mode so normally a savepoint would be taken
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // Set initialSavepointPath and bump savepointRedeployNonce
        String userSpecifiedSavepoint = "s3://bucket/my-specific-savepoint";
        rs.deployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setInitialSavepointPath(userSpecifiedSavepoint);
        rs.deployment.getSpec().getTemplate().getSpec().getJob().setSavepointRedeployNonce(12345L);
        rs.deployment
                .getSpec()
                .getConfiguration()
                .put(DEPLOYMENT_DELETION_DELAY.key(), String.valueOf(ALT_DELETION_DELAY_VALUE));
        kubernetesClient.resource(rs.deployment).createOrReplace();

        // Reconcile - should skip savepointing and go directly to transition
        rs = reconcile(rs.deployment);

        // Verify: Should be TRANSITIONING_TO_GREEN (not SAVEPOINTING_BLUE)
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState());

        // Verify: Green deployment should use the user-specified initialSavepointPath
        var flinkDeployments = getFlinkDeployments();
        assertEquals(2, flinkDeployments.size());
        assertEquals(
                userSpecifiedSavepoint,
                flinkDeployments.get(1).getSpec().getJob().getInitialSavepointPath());

        // Complete the transition
        simulateSuccessfulJobStart(flinkDeployments.get(1));
        rs = reconcile(rs.deployment);

        // Wait for deletion delay
        Thread.sleep(rs.updateControl.getScheduleDelay().get());
        reconcile(rs.deployment);

        // Verify Green is now active
        flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());

        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySuspendAndResumeInPlace(FlinkVersion flinkVersion) throws Exception {
        var rs = setupActiveBlueDeployment(flinkVersion);
        assertEquals(1, getFlinkDeployments().size());
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "Should start as ACTIVE_BLUE");

        // Suspend in-place with spec change (parallelism bump)
        var deployment = rs.deployment;
        deployment.getSpec().getTemplate().getSpec().getJob().setState(JobState.SUSPENDED);
        deployment.getSpec().getTemplate().getSpec().getJob().setParallelism(5);
        kubernetesClient.resource(deployment).createOrReplace();

        rs = reconcile(deployment);
        deployment = rs.deployment;
        // Verify BG goes through TRANSITIONING_TO_BLUE state (unified patch mechanism)
        assertEquals(
                JobStatus.RECONCILING,
                rs.reconciledStatus.getJobStatus().getState(),
                "BG status should be RECONCILING after suspend initiated");
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "Should go through TRANSITIONING_TO_BLUE during suspend");

        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size(), "Suspend should keep single child");
        var child = flinkDeployments.get(0);
        assertTrue(
                child.getMetadata().getName().endsWith("-blue"), "Child should be blue deployment");
        assertEquals(
                JobState.SUSPENDED,
                child.getSpec().getJob().getState(),
                "Child should be suspended");
        assertEquals(5, child.getSpec().getJob().getParallelism(), "Spec change should be applied");

        // Simulate child becoming suspended (lifecycleState is computed from lastReconciledSpec)
        simulateSuccessfulSuspend(child);
        rs = reconcile(deployment);
        deployment = rs.deployment;
        assertEquals(JobStatus.SUSPENDED, rs.reconciledStatus.getJobStatus().getState());
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "Should finalize to ACTIVE_BLUE when suspended");

        // Resume in-place with another spec change
        deployment.getSpec().getTemplate().getSpec().getJob().setState(JobState.RUNNING);
        deployment.getSpec().getTemplate().getSpec().getJob().setParallelism(6);
        kubernetesClient.resource(deployment).createOrReplace();

        rs = reconcile(deployment);
        deployment = rs.deployment;
        // Resume also goes through TRANSITIONING_TO_BLUE (unified patch mechanism)
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "Should go through TRANSITIONING_TO_BLUE during resume");
        // Persist the BG status update to kubernetes for the next reconcile
        kubernetesClient.resource(deployment).updateStatus();

        flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size(), "Resume should keep single child");
        child = flinkDeployments.get(0);
        assertTrue(
                child.getMetadata().getName().endsWith("-blue"),
                "Child should still be blue deployment");
        assertEquals(JobState.RUNNING, child.getSpec().getJob().getState(), "Child should resume");
        assertEquals(6, child.getSpec().getJob().getParallelism(), "Latest spec should be applied");

        // Simulate child becoming running and verify BG status syncs
        simulateSuccessfulJobStart(child);
        rs = reconcile(deployment);
        assertEquals(JobStatus.RUNNING, rs.reconciledStatus.getJobStatus().getState());
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "Should finalize to ACTIVE_BLUE after resume");
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySuspendDuringTransitionIsBlockedUntilComplete(FlinkVersion flinkVersion)
            throws Exception {
        // Start with a running Blue deployment with longer abort grace period
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.STATELESS); // STATELESS to skip savepointing
        // Set longer abort grace period BEFORE deployment to avoid abort during transition test
        blueGreenDeployment.getSpec().getConfiguration().put(ABORT_GRACE_PERIOD.key(), "60000");
        // Use zero deletion delay to avoid timing-based flakiness during transition completion
        blueGreenDeployment.getSpec().getConfiguration().put(DEPLOYMENT_DELETION_DELAY.key(), "0");
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // === TRIGGER TRANSITION ===
        rs.deployment.getSpec().getTemplate().getSpec().getJob().setParallelism(5);
        kubernetesClient.resource(rs.deployment).createOrReplace();

        // Reconcile - start transition to GREEN
        rs = reconcile(rs.deployment);

        // GREEN deployment should be created
        var flinkDeployments = getFlinkDeployments();
        assertEquals(2, flinkDeployments.size());

        // === SUSPEND DURING TRANSITION ===
        rs.deployment.getSpec().getTemplate().getSpec().getJob().setState(JobState.SUSPENDED);
        kubernetesClient.resource(rs.deployment).createOrReplace();

        // Reconcile - suspend should be BLOCKED, transition continues
        rs = reconcile(rs.deployment);

        // GREEN deployment should NOT be suspended yet
        flinkDeployments = getFlinkDeployments();
        var greenDeployment =
                flinkDeployments.stream()
                        .filter(fd -> fd.getMetadata().getName().endsWith("-green"))
                        .findFirst()
                        .orElseThrow();
        assertEquals(JobState.RUNNING, greenDeployment.getSpec().getJob().getState());

        // === TRANSITION COMPLETES ===
        // Simulate GREEN becoming ready
        simulateSuccessfulJobStart(greenDeployment);

        // Reconcile to complete transition and process pending suspend
        // (sets timestamp, deletes BLUE, finalizes to ACTIVE_GREEN, then processes suspend
        // via TRANSITIONING_TO_GREEN)
        for (int i = 0; i < 5; i++) {
            rs = reconcile(rs.deployment);
        }
        // Persist the BG status update to kubernetes for the next reconcile
        kubernetesClient.resource(rs.deployment).updateStatus();

        // GREEN should now be suspended in spec (via patchFlinkDeployment)
        flinkDeployments = getFlinkDeployments();
        greenDeployment =
                flinkDeployments.stream()
                        .filter(fd -> fd.getMetadata().getName().endsWith("-green"))
                        .findFirst()
                        .orElseThrow();
        assertEquals(JobState.SUSPENDED, greenDeployment.getSpec().getJob().getState());

        // Simulate GREEN child becoming suspended (lifecycleState is computed from
        // lastReconciledSpec)
        simulateSuccessfulSuspend(greenDeployment);

        // Reconcile - finalizeSuspendedDeployment should set BG job status to SUSPENDED
        rs = reconcile(rs.deployment);
        assertEquals(JobStatus.SUSPENDED, rs.reconciledStatus.getJobStatus().getState());
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyFailureBeforeTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        TEST_INITIAL_SAVEPOINT_PATH,
                        UpgradeMode.STATELESS);
        var rs =
                executeBasicDeployment(
                        flinkVersion, blueGreenDeployment, false, TEST_INITIAL_SAVEPOINT_PATH);

        // Simulate a change in the spec to trigger a Blue deployment
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);

        // Simulate a failure in the running deployment
        simulateJobFailure(getFlinkDeployments().get(0));

        // Initiate the Green deployment
        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(
                minReconciliationTs
                        < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));

        assertFailingJobStatus(rs);

        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        assertEquals(
                JobStatus.RECONCILING,
                flinkDeployments.get(0).getStatus().getJobStatus().getState());
        assertEquals(
                ReconciliationState.UPGRADING,
                flinkDeployments.get(0).getStatus().getReconciliationStatus().getState());

        // No update
        rs = reconcile(rs.deployment);
        assertTrue(rs.updateControl.isNoUpdate());
    }

    @ParameterizedTest
    @MethodSource({"org.apache.flink.kubernetes.operator.TestUtils#flinkVersions"})
    public void verifyFailureDuringTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.STATELESS);

        // Overriding the maxNumRetries and Reschedule Interval
        var abortGracePeriodMs = 1200;
        var reconciliationReschedulingIntervalMs = 3000;
        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(ABORT_GRACE_PERIOD.key(), String.valueOf(abortGracePeriodMs));
        configuration.put(
                RECONCILIATION_RESCHEDULING_INTERVAL.key(),
                String.valueOf(reconciliationReschedulingIntervalMs));

        var rs =
                executeBasicDeployment(
                        flinkVersion, blueGreenDeployment, false, TEST_INITIAL_SAVEPOINT_PATH);

        // Simulate a change in the spec to trigger a Blue deployment
        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue, 0, null);

        // Initiate the Green deployment
        rs = reconcile(rs.deployment);

        // We should be TRANSITIONING_TO_GREEN at this point
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState());
        assertEquals(
                customValue,
                getFlinkConfigurationValue(
                        rs.deployment.getSpec().getTemplate().getSpec(), CUSTOM_CONFIG_FIELD));

        // Simulating the Blue deployment doesn't start correctly (status will remain the same)
        Long reschedDelayMs = 0L;
        for (int i = 0; i < 2; i++) {
            rs = reconcile(rs.deployment);
            assertTrue(rs.updateControl.isPatchStatus());
            assertFalse(rs.updateControl.isPatchResource());
            assertTrue(rs.updateControl.getScheduleDelay().isPresent());
            reschedDelayMs = rs.updateControl.getScheduleDelay().get();
            assertTrue(
                    reschedDelayMs == reconciliationReschedulingIntervalMs && reschedDelayMs > 0);
            assertTrue(
                    instantStrToMillis(rs.reconciledStatus.getAbortTimestamp())
                            > System.currentTimeMillis());
        }

        // Wait until the delay
        Thread.sleep(reschedDelayMs);

        // After the retries are exhausted
        rs = reconcile(rs.deployment);

        assertTrue(rs.updateControl.isPatchStatus());

        // The first job should be RUNNING, the second should be SUSPENDED
        assertFailingJobStatus(rs);
        // No longer TRANSITIONING_TO_GREEN and rolled back to ACTIVE_BLUE
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE, rs.reconciledStatus.getBlueGreenState());
        var flinkDeployments = getFlinkDeployments();
        assertEquals(2, flinkDeployments.size());
        assertEquals(
                JobStatus.RUNNING, flinkDeployments.get(0).getStatus().getJobStatus().getState());
        assertEquals(
                ReconciliationState.DEPLOYED,
                flinkDeployments.get(0).getStatus().getReconciliationStatus().getState());
        // The B/G controller changes the State = SUSPENDED, the actual suspension is done by the
        // FlinkDeploymentController
        assertEquals(JobState.SUSPENDED, flinkDeployments.get(1).getSpec().getJob().getState());
        assertEquals(
                ReconciliationState.UPGRADING,
                flinkDeployments.get(1).getStatus().getReconciliationStatus().getState());
        // Cleared with the rest of the transition markers: an ended transition must not leave a
        // clock behind for the next one to inherit (matches upstream main).
        assertEquals(0, instantStrToMillis(rs.reconciledStatus.getAbortTimestamp()));
        // savepointTriggerId must be cleared on abort so the next transition
        // triggers a fresh savepoint instead of reusing a stale triggerId
        assertNull(rs.reconciledStatus.getSavepointTriggerId());

        // Simulate another change in the spec to trigger a redeployment
        customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue, ALT_DELETION_DELAY_VALUE, null);

        // Initiate the redeployment
        testTransitionToGreen(rs, customValue, null);
    }

    private static String getFlinkConfigurationValue(
            FlinkDeploymentSpec flinkDeploymentSpec, String propertyName) {
        return flinkDeploymentSpec.getFlinkConfiguration().get(propertyName).asText();
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySpecChangeDuringTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        TEST_INITIAL_SAVEPOINT_PATH,
                        UpgradeMode.STATELESS);

        // Initiate the Blue deployment
        var originalSpec = blueGreenDeployment.getSpec();
        var rs = initialPhaseBasicDeployment(blueGreenDeployment, false);

        // Job starting...
        simulateSubmitAndSuccessfulJobStart(getFlinkDeployments().get(0));

        // Simulate a spec change before the transition is complete
        simulateChangeInSpec(rs.deployment, "MODIFIED_VALUE", 0, null);
        var moddedSpec = rs.deployment.getSpec();
        rs = reconcile(rs.deployment);

        // The spec change should have been preserved
        assertNotEquals(
                SpecUtils.writeSpecAsJSON(originalSpec, "spec"),
                SpecUtils.writeSpecAsJSON(rs.deployment.getSpec(), "spec"));

        assertEquals(
                SpecUtils.writeSpecAsJSON(moddedSpec, "spec"),
                rs.deployment.getStatus().getLastReconciledSpec(),
                "spec");
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyFailureBeforeFirstDeployment(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        TEST_INITIAL_SAVEPOINT_PATH,
                        UpgradeMode.STATELESS);

        // Initiate the Blue deployment
        var rs = initialPhaseBasicDeployment(blueGreenDeployment, false);

        // Simulating the job did not start correctly before the AbortGracePeriodMs
        Thread.sleep(MINIMUM_ABORT_GRACE_PERIOD);

        rs = reconcile(rs.deployment);

        assertFailingJobStatus(rs);

        // No longer TRANSITIONING_TO_GREEN and rolled back to INITIALIZING_BLUE
        assertEquals(
                FlinkBlueGreenDeploymentState.INITIALIZING_BLUE,
                rs.reconciledStatus.getBlueGreenState());
        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        // The B/G controller changes the State = SUSPENDED, the actual suspension is done by the
        // FlinkDeploymentController
        assertEquals(JobState.SUSPENDED, flinkDeployments.get(0).getSpec().getJob().getState());

        // No-op if the spec remains the same
        rs = reconcile(rs.deployment);
        assertTrue(rs.updateControl.isNoUpdate());

        simulateChangeInSpec(rs.deployment, "MODIFIED_VALUE", 0, null);

        // Resubmitting should re-start the Initialization to Blue
        rs = reconcile(rs.deployment);

        // Any error should've been cleaned up
        assertNull(rs.reconciledStatus.getError());
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(
                rs.updateControl.getScheduleDelay().isPresent()
                        && rs.updateControl.getScheduleDelay().get() > 0);
        flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                rs.reconciledStatus.getBlueGreenState());
    }

    @ParameterizedTest
    @MethodSource("patchScenarioProvider")
    public void verifyPatchScenario(FlinkVersion flinkVersion, PatchTestCase testCase)
            throws Exception {
        var rs = setupActiveBlueDeployment(flinkVersion);

        testCase.applyChanges(rs.deployment, kubernetesClient);

        // PatchTopLevelTestCase should now be ignored (return noUpdate)
        if (testCase instanceof PatchTopLevelTestCase) {
            var result = reconcileAndVerifyIgnoreBehavior(rs);
            testCase.verifySpecificBehavior(result, getFlinkDeployments());
        } else {
            var result = reconcileAndVerifyPatchBehavior(rs);
            testCase.verifySpecificBehavior(result, getFlinkDeployments());
            assertFinalized(
                    result.minReconciliationTs,
                    result.rs,
                    FlinkBlueGreenDeploymentState.ACTIVE_BLUE);
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySavepointFailureRecovery(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.LAST_STATE);

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // First attempt: Configure service to throw exception
        flinkService.setSavepointTriggerException(
                new IllegalStateException("Job not in valid state for savepoint"));

        String customValue = UUID.randomUUID().toString();
        simulateSpecChange(rs.deployment, customValue);

        // Should fail with savepoint error
        rs = reconcile(rs.deployment);
        assertFailingWithError(rs, "Job not in valid state for savepoint");

        // Recovery: Clear the exception and try again with new spec change
        flinkService.clearSavepointTriggerException();
        customValue = UUID.randomUUID().toString() + "_recovery";
        simulateChangeInSpec(rs.deployment, customValue, ALT_DELETION_DELAY_VALUE, null);

        // Should now succeed and trigger savepoint properly
        rs = handleSavepoint(rs);

        // Continue with successful transition
        testTransitionToGreen(rs, customValue, "savepoint_1");
    }

    @ParameterizedTest
    @MethodSource("savepointExceptionProvider")
    public void verifySavepointFailureWithDifferentExceptionTypes(
            FlinkVersion flinkVersion, Exception savepointException) throws Exception {

        String expectedErrorFragment = savepointException.getMessage();

        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        flinkService.setSavepointTriggerException(savepointException);
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);

        rs = reconcile(rs.deployment);

        assertFailingJobStatus(rs);
        assertTrue(rs.reconciledStatus.getError().contains("Could not trigger Savepoint"));
        assertTrue(rs.reconciledStatus.getError().contains(expectedErrorFragment));

        // Should remain in ACTIVE_BLUE state (no transition started)
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE, rs.reconciledStatus.getBlueGreenState());

        // Verify only Blue deployment exists (Green was never created)
        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySavepointFetchFailureRecovery(FlinkVersion flinkVersion) throws Exception {
        String error = "Savepoint corrupted or not found";

        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        String customValue = UUID.randomUUID().toString();
        simulateSpecChange(rs.deployment, customValue);

        // Trigger savepoint successfully and go through savepointing flow
        rs = handleSavepoint(rs);

        // Configure service to return fetch error
        flinkService.setSavepointFetchError(error);

        // The next reconciliation will fail in configureInitialSavepoint due to fetch error
        rs = reconcile(rs.deployment);
        assertFailingWithError(rs, "Could not start Transition", error);

        // Recovery: Clear the fetch error and try again with new spec change
        flinkService.clearSavepointFetchError();
        customValue = UUID.randomUUID().toString() + "_recovery";
        simulateChangeInSpec(rs.deployment, customValue, ALT_DELETION_DELAY_VALUE, null);

        // Should now succeed and complete transition properly
        rs = handleSavepoint(rs);

        // Continue with successful transition - second savepoint will be "savepoint_2"
        testTransitionToGreen(rs, customValue, "savepoint_2");
    }

    @ParameterizedTest
    @MethodSource("savepointErrorProvider")
    public void verifySavepointFetchFailureWithDifferentErrors(
            FlinkVersion flinkVersion, String errorMessage, boolean isFetchError) throws Exception {

        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        simulateSpecChange(rs.deployment, UUID.randomUUID().toString());

        if (isFetchError) {
            // Trigger savepoint successfully and go through savepointing flow
            rs = handleSavepoint(rs);

            // Configure service to return fetch error
            flinkService.setSavepointFetchError(errorMessage);

            // The next reconciliation will fail in configureInitialSavepoint due to fetch error
            rs = reconcile(rs.deployment);
            assertFailingWithError(rs, "Could not start Transition", errorMessage);
        } else {
            // Configure service to throw trigger exception
            flinkService.setSavepointTriggerException(new RuntimeException(errorMessage));

            rs = reconcile(rs.deployment);
            assertFailingWithError(rs, "Could not trigger Savepoint", errorMessage);
        }

        // Should remain in ACTIVE_BLUE state after failure
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE, rs.reconciledStatus.getBlueGreenState());

        // Only Blue deployment should exist (Green transition never started)
        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySuspendWhenChildNotReadyPatchesWithoutFailing(FlinkVersion flinkVersion)
            throws Exception {
        var rs = setupActiveBlueDeployment(flinkVersion);

        // Mark the active child as not ready
        var child = getFlinkDeployments().get(0);
        child.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
        kubernetesClient.resource(child).update();

        // Suspend and bump parallelism
        var deployment = rs.deployment;
        deployment.getSpec().getTemplate().getSpec().getJob().setState(JobState.SUSPENDED);
        deployment.getSpec().getTemplate().getSpec().getJob().setParallelism(9);
        kubernetesClient.resource(deployment).createOrReplace();

        rs = reconcile(deployment);

        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size(), "Should not create new child");
        child = flinkDeployments.get(0);
        assertEquals(JobState.SUSPENDED, child.getSpec().getJob().getState());
        assertEquals(9, child.getSpec().getJob().getParallelism(), "Spec change should apply");
        assertNotEquals(
                JobStatus.FAILING,
                rs.reconciledStatus.getJobStatus().getState(),
                "Should not mark parent failing when suspending not-ready child");
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyInitialSuspendedIsBlockedThenDeploysOnRunning(FlinkVersion flinkVersion)
            throws Exception {
        var bg =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.STATELESS);
        bg.getSpec().getTemplate().getSpec().getJob().setState(JobState.SUSPENDED);
        kubernetesClient.resource(bg).createOrReplace();

        // First reconcile initializes status to INITIALIZING_BLUE
        var rs = reconcile(bg);
        // Second reconcile: handler detects SUSPENDED and blocks
        rs = reconcile(rs.deployment);

        // Block child creation when initial state is SUSPENDED
        assertEquals(0, getFlinkDeployments().size(), "No child should be created");
        assertEquals(
                JobStatus.SUSPENDED,
                rs.reconciledStatus.getJobStatus().getState(),
                "Job status should be SUSPENDED when initial deployment is blocked");

        // Flip to RUNNING and reconcile again
        bg = rs.deployment;
        bg.getSpec().getTemplate().getSpec().getJob().setState(JobState.RUNNING);
        kubernetesClient.resource(bg).createOrReplace();

        rs = reconcile(bg);

        assertEquals(1, getFlinkDeployments().size(), "Child should be created after fix");
        assertEquals(
                JobStatus.RECONCILING,
                rs.reconciledStatus.getJobStatus().getState(),
                "Job status should be RECONCILING after deployment initiated");
    }

    // ==================== Parameterized Test Inputs ====================

    static Stream<Arguments> savepointErrorProvider() {
        return TestUtils.flinkVersions()
                .flatMap(
                        flinkVersionArgs -> {
                            FlinkVersion version = (FlinkVersion) flinkVersionArgs.get()[0];
                            return Stream.of(
                                    // Fetch errors
                                    Arguments.of(version, "Savepoint file corrupted", true),
                                    Arguments.of(version, "Storage system unavailable", true),
                                    Arguments.of(
                                            version, "Access denied to savepoint location", true),
                                    Arguments.of(version, "Savepoint metadata missing", true),
                                    // Trigger exceptions
                                    Arguments.of(version, "Network timeout", false),
                                    Arguments.of(version, "Job not running", false),
                                    Arguments.of(version, "Service unavailable", false),
                                    Arguments.of(version, "Generic error", false));
                        });
    }

    static Stream<Arguments> savepointExceptionProvider() {
        return TestUtils.flinkVersions()
                .flatMap(
                        flinkVersionArgs -> {
                            FlinkVersion version = (FlinkVersion) flinkVersionArgs.get()[0];
                            return Stream.of(
                                    Arguments.of(version, new IOException("Network timeout")),
                                    Arguments.of(
                                            version, new IllegalStateException("Job not running")),
                                    Arguments.of(
                                            version, new RuntimeException("Service unavailable")),
                                    Arguments.of(version, new Exception("Generic error")));
                        });
    }

    static Stream<Arguments> patchScenarioProvider() {
        // Extract FlinkVersions from TestUtils and combine with PatchTypes
        return TestUtils.flinkVersions()
                .flatMap(
                        args -> {
                            FlinkVersion version = (FlinkVersion) args.get()[0];
                            return Stream.of(
                                    Arguments.of(version, new PatchChildTestCase()),
                                    Arguments.of(version, new PatchTopLevelTestCase()),
                                    Arguments.of(version, new PatchBothTestCase()));
                        });
    }

    static Stream<Arguments> flinkVersionsAndSavepointPaths() {
        return TestUtils.flinkVersions()
                .flatMap(
                        args -> {
                            FlinkVersion version = (FlinkVersion) args.get()[0];
                            return Stream.of(
                                    Arguments.of(version, null),
                                    Arguments.of(version, TEST_INITIAL_SAVEPOINT_PATH));
                        });
    }

    static Stream<Arguments> flinkVersionsAndSavepointPathsAndUpgradeModes() {
        return TestUtils.flinkVersions()
                .flatMap(
                        args -> {
                            FlinkVersion version = (FlinkVersion) args.get()[0];
                            return Stream.of(
                                    Arguments.of(version, null, UpgradeMode.SAVEPOINT),
                                    Arguments.of(version, null, UpgradeMode.LAST_STATE),
                                    Arguments.of(version, null, UpgradeMode.STATELESS),
                                    Arguments.of(
                                            version,
                                            TEST_INITIAL_SAVEPOINT_PATH,
                                            UpgradeMode.SAVEPOINT),
                                    Arguments.of(
                                            version,
                                            TEST_INITIAL_SAVEPOINT_PATH,
                                            UpgradeMode.LAST_STATE),
                                    Arguments.of(
                                            version,
                                            TEST_INITIAL_SAVEPOINT_PATH,
                                            UpgradeMode.STATELESS));
                        });
    }

    // ==================== Test Case Interfaces and Implementations ====================

    interface PatchTestCase {
        void applyChanges(FlinkBlueGreenDeployment deployment, KubernetesClient client);

        void verifySpecificBehavior(ReconcileResult result, List<FlinkDeployment> deployments);
    }

    static class PatchChildTestCase implements PatchTestCase {
        @Override
        public void applyChanges(FlinkBlueGreenDeployment deployment, KubernetesClient client) {
            FlinkDeploymentSpec spec = deployment.getSpec().getTemplate().getSpec();

            // Add a configuration change that ReflectiveDiffBuilder considers ignorable
            spec.getFlinkConfiguration()
                    .put("kubernetes.operator.reconcile.interval", "100 SECONDS");

            deployment.getSpec().getTemplate().setSpec(spec);
            client.resource(deployment).createOrReplace();
        }

        @Override
        public void verifySpecificBehavior(
                ReconcileResult result, List<FlinkDeployment> deployments) {
            assertEquals(1, deployments.size());
            assertEquals(
                    "100 SECONDS",
                    getFlinkConfigurationValue(
                            deployments.get(0).getSpec(),
                            "kubernetes.operator.reconcile.interval"));
        }
    }

    static class PatchTopLevelTestCase implements PatchTestCase {
        @Override
        public void applyChanges(FlinkBlueGreenDeployment deployment, KubernetesClient client) {
            FlinkBlueGreenDeploymentSpec bgSpec = deployment.getSpec();
            FlinkDeploymentTemplateSpec template = bgSpec.getTemplate();
            Map<String, String> configuration = new HashMap<>(bgSpec.getConfiguration());
            configuration.put("custom.top.level", "custom-top-level-value");
            bgSpec.setConfiguration(configuration);
            bgSpec.setTemplate(template);
            client.resource(deployment).createOrReplace();
        }

        @Override
        public void verifySpecificBehavior(
                ReconcileResult result, List<FlinkDeployment> deployments) {
            assertEquals(1, deployments.size());
            var existingDeployment = result.existingFlinkDeployment;
            var currentDeployment = deployments.get(0);

            // FlinkDeployment should remain unchanged for top-level only changes
            assertEquals(existingDeployment, currentDeployment);
        }
    }

    static class PatchBothTestCase implements PatchTestCase {
        @Override
        public void applyChanges(FlinkBlueGreenDeployment deployment, KubernetesClient client) {
            FlinkBlueGreenDeploymentSpec bgSpec = deployment.getSpec();
            FlinkDeploymentTemplateSpec template = bgSpec.getTemplate();

            // 1. Add top-level configuration change
            Map<String, String> configuration = new HashMap<>(bgSpec.getConfiguration());
            configuration.put("custom.both.level", "custom-both-level-value");
            bgSpec.setConfiguration(configuration);

            // 2. Add nested spec change
            FlinkDeploymentSpec spec = template.getSpec();
            spec.getFlinkConfiguration()
                    .put("kubernetes.operator.reconcile.interval", "100 SECONDS");
            template.setSpec(spec);

            bgSpec.setTemplate(template);
            client.resource(deployment).createOrReplace();
        }

        @Override
        public void verifySpecificBehavior(
                ReconcileResult result, List<FlinkDeployment> deployments) {
            assertEquals(1, deployments.size());

            // Child spec change should be applied to FlinkDeployment
            assertEquals(
                    "100 SECONDS",
                    getFlinkConfigurationValue(
                            deployments.get(0).getSpec(),
                            "kubernetes.operator.reconcile.interval"));

            // Top-level changes should be preserved in reconciled spec
            assertNotNull(result.rs.reconciledStatus.getLastReconciledSpec());
            assertEquals(
                    SpecUtils.writeSpecAsJSON(result.rs.deployment.getSpec(), "spec"),
                    result.rs.reconciledStatus.getLastReconciledSpec());
        }
    }

    // ==================== Helper Classes ====================

    static class ReconcileResult {
        final long minReconciliationTs;
        final TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs;
        final FlinkDeployment existingFlinkDeployment;

        ReconcileResult(
                long minReconciliationTs,
                TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
                FlinkDeployment existingFlinkDeployment) {
            this.minReconciliationTs = minReconciliationTs;
            this.rs = rs;
            this.existingFlinkDeployment = existingFlinkDeployment;
        }
    }

    // ==================== Common Test Helper Methods ====================

    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
            setupActiveBlueDeployment(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        TEST_INITIAL_SAVEPOINT_PATH,
                        UpgradeMode.STATELESS);
        return executeBasicDeployment(
                flinkVersion, blueGreenDeployment, false, TEST_INITIAL_SAVEPOINT_PATH);
    }

    private ReconcileResult reconcileAndVerifyPatchBehavior(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs)
            throws Exception {

        // Initiating a patch operation
        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertPatchOperationTriggered(rs, minReconciliationTs);
        assertTransitioningState(rs);

        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());

        // The patch operation reinitialized the deployment, simulating startup
        simulateSuccessfulJobStart(flinkDeployments.get(0));

        minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        return new ReconcileResult(minReconciliationTs, rs, null);
    }

    private ReconcileResult reconcileAndVerifyIgnoreBehavior(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs)
            throws Exception {

        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        var existingFlinkDeployment = flinkDeployments.get(0);

        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertIgnoreOperationTriggered(rs);

        return new ReconcileResult(minReconciliationTs, rs, existingFlinkDeployment);
    }

    private void assertIgnoreOperationTriggered(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs) {
        // For IGNORE behavior, we expect noUpdate (no patch status, no reschedule)
        assertFalse(rs.updateControl.isPatchStatus());
        assertFalse(rs.updateControl.isPatchResource());
        assertFalse(rs.updateControl.getScheduleDelay().isPresent());
    }

    private void assertPatchOperationTriggered(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            long minReconciliationTs) {
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(rs.updateControl.getScheduleDelay().isPresent());
        assertTrue(rs.updateControl.getScheduleDelay().get() > 0);
        assertTrue(
                minReconciliationTs
                        < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));
    }

    private void assertTransitioningState(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs) {
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                rs.reconciledStatus.getBlueGreenState());
        assertEquals(JobStatus.RECONCILING, rs.reconciledStatus.getJobStatus().getState());
    }

    private static void assertFailingJobStatus(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs) {
        assertEquals(JobStatus.FAILING, rs.reconciledStatus.getJobStatus().getState());
        assertNotNull(rs.reconciledStatus.getError());
    }

    private static void assertFailingWithError(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            String... expectedErrorFragments) {
        assertFailingJobStatus(rs);
        for (String fragment : expectedErrorFragments) {
            assertTrue(rs.reconciledStatus.getError().contains(fragment));
        }
    }

    private void assertFinalized(
            long minReconciliationTs,
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            FlinkBlueGreenDeploymentState expectedBGDeploymentState)
            throws Exception {
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(
                minReconciliationTs
                        < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));
        assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(
                SpecUtils.writeSpecAsJSON(rs.deployment.getSpec(), "spec"),
                rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(expectedBGDeploymentState, rs.reconciledStatus.getBlueGreenState());
        assertEquals(JobStatus.RUNNING, rs.reconciledStatus.getJobStatus().getState());
        assertEquals(0, instantStrToMillis(rs.reconciledStatus.getDeploymentReadyTimestamp()));
        assertEquals(0, instantStrToMillis(rs.reconciledStatus.getAbortTimestamp()));

        // Subsequent reconciliation calls after finalization = NO-OP
        rs = reconcile(rs.deployment);
        assertTrue(rs.updateControl.isNoUpdate());
    }

    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
            executeBasicDeployment(
                    FlinkVersion flinkVersion,
                    FlinkBlueGreenDeployment blueGreenDeployment,
                    boolean execAssertions,
                    String expectedInitialSavepointPath)
                    throws Exception {

        // 1. Initiate the Blue deployment
        var rs = initialPhaseBasicDeployment(blueGreenDeployment, execAssertions);

        var flinkDeployments = getFlinkDeployments();
        var deploymentA = flinkDeployments.get(0);

        if (execAssertions) {
            assertEquals(1, flinkDeployments.size());
            verifyOwnerReferences(rs.deployment, deploymentA);
            assertEquals(
                    expectedInitialSavepointPath,
                    deploymentA.getSpec().getJob().getInitialSavepointPath());
        }

        simulateSubmitAndSuccessfulJobStart(deploymentA);

        // 2. Mark the Blue deployment ready and finalize it
        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        if (execAssertions) {
            assertFinalized(minReconciliationTs, rs, FlinkBlueGreenDeploymentState.ACTIVE_BLUE);
        }

        return rs;
    }

    @NotNull
    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
            initialPhaseBasicDeployment(
                    FlinkBlueGreenDeployment blueGreenDeployment, boolean execAssertions)
                    throws Exception {
        Long minReconciliationTs = System.currentTimeMillis() - 1;

        // Create the resource in the mock server before reconciling
        kubernetesClient.resource(blueGreenDeployment).createOrReplace();

        // 1a. Initializing deploymentStatus with this call
        var rs = reconcile(blueGreenDeployment);

        if (execAssertions) {
            assertTrue(rs.updateControl.getScheduleDelay().isPresent());
            assertEquals(0, (long) rs.updateControl.getScheduleDelay().get());
            assertEquals(
                    FlinkBlueGreenDeploymentState.INITIALIZING_BLUE,
                    rs.reconciledStatus.getBlueGreenState());
        }

        // 1b. Executing the actual deployment
        rs = reconcile(rs.deployment);

        if (execAssertions) {
            assertTrue(rs.updateControl.isPatchStatus());
            assertTrue(
                    minReconciliationTs
                            < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));

            // check the status (reconciled spec, reconciled ts, a/b state)
            assertEquals(
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                    rs.reconciledStatus.getBlueGreenState());
            assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
            assertEquals(JobStatus.RECONCILING, rs.reconciledStatus.getJobStatus().getState());
            assertNull(rs.reconciledStatus.getDeploymentReadyTimestamp());
        }

        return rs;
    }

    private void assertDeploymentDeleted(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            long expectedDeletionDelay,
            FlinkBlueGreenDeploymentSpec bgSpecBefore)
            throws Exception {
        var deletionDelay = rs.updateControl.getScheduleDelay().get();

        assertTrue(rs.updateControl.isPatchStatus());
        assertEquals(expectedDeletionDelay, deletionDelay);
        assertTrue(instantStrToMillis(rs.reconciledStatus.getDeploymentReadyTimestamp()) > 0);
        assertEquals(
                SpecUtils.writeSpecAsJSON(bgSpecBefore, "spec"),
                rs.reconciledStatus.getLastReconciledSpec());

        // A reconciliation before the deletion delay has expired should result in no-op
        rs = reconcile(rs.deployment);
        var remainingDeletionDelay = rs.updateControl.getScheduleDelay().get();
        assertTrue(remainingDeletionDelay <= expectedDeletionDelay);
        assertTrue(rs.updateControl.isNoUpdate());

        Thread.sleep(remainingDeletionDelay);
    }

    private void testTransitionToGreen(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            String customValue,
            String expectedSavepointPath)
            throws Exception {

        // Initiate the Green deployment
        Long minReconciliationTs = System.currentTimeMillis() - 1;
        var bgSpecBefore = rs.deployment.getSpec();
        rs = reconcile(rs.deployment);

        var flinkDeployments = getFlinkDeployments();
        var greenDeploymentName = flinkDeployments.get(1).getMetadata().getName();

        // Any error should've been cleaned up
        assertNull(rs.reconciledStatus.getError());
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(
                minReconciliationTs
                        < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));
        assertEquals(2, flinkDeployments.size());
        assertNull(flinkDeployments.get(0).getSpec().getJob().getInitialSavepointPath());
        assertEquals(
                expectedSavepointPath,
                flinkDeployments.get(1).getSpec().getJob().getInitialSavepointPath());

        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState());
        assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(0, instantStrToMillis(rs.reconciledStatus.getDeploymentReadyTimestamp()));
        assertEquals(
                customValue,
                getFlinkConfigurationValue(
                        rs.deployment.getSpec().getTemplate().getSpec(), CUSTOM_CONFIG_FIELD));

        // Initiate and mark the Green deployment ready
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        // Logic for the deployment to get deleted
        assertDeploymentDeleted(rs, ALT_DELETION_DELAY_VALUE, bgSpecBefore);

        // Calling the rescheduled reconciliation (will delete the deployment)
        reconcile(rs.deployment);

        // Old Blue deployment deleted, Green is the active one
        flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        assertEquals(greenDeploymentName, flinkDeployments.get(0).getMetadata().getName());

        minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertTrue(rs.updateControl.isPatchStatus());
        assertFinalized(minReconciliationTs, rs, FlinkBlueGreenDeploymentState.ACTIVE_GREEN);
    }

    private void simulateChangeInSpec(
            FlinkBlueGreenDeployment blueGreenDeployment,
            String customFieldValue,
            int customDeletionDelayMs,
            String initialSavepointPath) {
        FlinkBlueGreenDeploymentSpec bgSpec = blueGreenDeployment.getSpec();
        FlinkDeploymentTemplateSpec template = bgSpec.getTemplate();

        if (customDeletionDelayMs > 0) {
            bgSpec.getConfiguration()
                    .put(DEPLOYMENT_DELETION_DELAY.key(), String.valueOf(customDeletionDelayMs));
        }

        FlinkDeploymentSpec spec = template.getSpec();
        spec.getFlinkConfiguration().put(CUSTOM_CONFIG_FIELD, customFieldValue);

        if (initialSavepointPath != null) {
            spec.getJob().setInitialSavepointPath(initialSavepointPath);
        }

        template.setSpec(spec);
        kubernetesClient.resource(blueGreenDeployment).createOrReplace();
    }

    private void simulateSpecChange(FlinkBlueGreenDeployment deployment, String customValue) {
        simulateChangeInSpec(deployment, customValue, 0, null);
    }

    /*
    Convenience function to reconcile and get the frequently used `BlueGreenReconciliationResult`
     */
    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult reconcile(
            FlinkBlueGreenDeployment blueGreenDeployment) throws Exception {
        UpdateControl<FlinkBlueGreenDeployment> updateControl =
                testController.reconcile(blueGreenDeployment, context);

        return new TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult(
                updateControl,
                updateControl.isNoUpdate()
                        ? blueGreenDeployment
                        : updateControl.getResource().get(),
                updateControl.isNoUpdate()
                        ? blueGreenDeployment.getStatus()
                        : updateControl.getResource().get().getStatus());
    }

    private void simulateSubmitAndSuccessfulJobStart(FlinkDeployment deployment) throws Exception {
        // TODO: is this correct? Doing this to give the TestingFlinkService awareness of the job
        JobSpec jobSpec = deployment.getSpec().getJob();
        Configuration conf = new Configuration();
        conf.set(SavepointConfigOptions.SAVEPOINT_PATH, TEST_CHECKPOINT_PATH);
        flinkService.submitApplicationCluster(jobSpec, conf, false);
        var jobId = flinkService.listJobs().get(0).f1.getJobId().toString();
        deployment.getStatus().getJobStatus().setJobId(jobId);
        simulateSuccessfulJobStart(deployment);
    }

    private void simulateSuccessfulJobStart(FlinkDeployment deployment) {
        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        deployment.getStatus().getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        deployment.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
        kubernetesClient.resource(deployment).update();
    }

    private void simulateSuccessfulSuspend(FlinkDeployment deployment) {
        deployment.getStatus().getJobStatus().setState(JobStatus.FINISHED);
        deployment.getStatus().getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        kubernetesClient.resource(deployment).update();
    }

    private void simulateJobFailure(FlinkDeployment deployment) {
        deployment.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
        deployment.getStatus().getReconciliationStatus().setState(ReconciliationState.UPGRADING);
        kubernetesClient.resource(deployment).update();
    }

    private static void verifyOwnerReferences(
            FlinkBlueGreenDeployment parent, FlinkDeployment child) {
        var ownerReferences = child.getMetadata().getOwnerReferences();
        assertEquals(1, ownerReferences.size());
        var ownerRef = ownerReferences.get(0);
        assertEquals(parent.getMetadata().getName(), ownerRef.getName());
        assertEquals(parent.getKind(), ownerRef.getKind());
        assertEquals(parent.getApiVersion(), ownerRef.getApiVersion());
    }

    private List<FlinkDeployment> getFlinkDeployments() {
        return kubernetesClient
                .resources(FlinkDeployment.class)
                .inNamespace(TEST_NAMESPACE)
                .list()
                .getItems();
    }

    private static FlinkBlueGreenDeployment buildSessionCluster(
            String name,
            String namespace,
            FlinkVersion version,
            String initialSavepointPath,
            UpgradeMode upgradeMode) {
        var deployment = new FlinkBlueGreenDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withCreationTimestamp(Instant.now().toString())
                        .withUid(UUID.randomUUID().toString())
                        .withResourceVersion("1")
                        .build());
        var bgDeploymentSpec = getTestFlinkDeploymentSpec(version);

        bgDeploymentSpec
                .getTemplate()
                .getSpec()
                .setJob(
                        JobSpec.builder()
                                .jarURI(SAMPLE_JAR)
                                .parallelism(1)
                                .upgradeMode(upgradeMode)
                                .state(JobState.RUNNING)
                                .initialSavepointPath(initialSavepointPath)
                                .build());

        deployment.setSpec(bgDeploymentSpec);
        return deployment;
    }

    private static FlinkBlueGreenDeploymentSpec getTestFlinkDeploymentSpec(FlinkVersion version) {
        Map<String, String> conf = new HashMap<>();
        conf.put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "2");
        conf.put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), "test-savepoint-dir");
        conf.put(CheckpointingOptions.INCREMENTAL_CHECKPOINTS.key(), "true");
        conf.put(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(), "10");
        conf.put(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
                "file:///test/test-checkpoint-dir");

        var flinkDeploymentSpec =
                FlinkDeploymentSpec.builder()
                        .image(IMAGE)
                        .imagePullPolicy(IMAGE_POLICY)
                        .serviceAccount(SERVICE_ACCOUNT)
                        .flinkVersion(version)
                        .flinkConfiguration(new ConfigObjectNode())
                        .jobManager(new JobManagerSpec(new Resource(1.0, "2048m", "2G"), 1, null))
                        .taskManager(
                                new TaskManagerSpec(new Resource(1.0, "2048m", "2G"), null, null))
                        .build();

        flinkDeploymentSpec.setFlinkConfiguration(conf);

        Map<String, String> configuration = new HashMap<>();
        configuration.put(ABORT_GRACE_PERIOD.key(), "1");
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "500");
        configuration.put(
                DEPLOYMENT_DELETION_DELAY.key(), String.valueOf(DEFAULT_DELETION_DELAY_VALUE));

        var flinkDeploymentTemplateSpec =
                FlinkDeploymentTemplateSpec.builder().spec(flinkDeploymentSpec).build();

        return new FlinkBlueGreenDeploymentSpec(configuration, null, flinkDeploymentTemplateSpec);
    }

    // ==================== Ingress Helper Methods ====================

    private void assertIngressPointsToService(String expectedServiceName) {
        if (IngressUtils.ingressInNetworkingV1(kubernetesClient)) {
            Ingress ingress = getIngressV1(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE);
            assertNotNull(ingress);
            assertEquals(
                    expectedServiceName,
                    ingress.getSpec()
                            .getRules()
                            .get(0)
                            .getHttp()
                            .getPaths()
                            .get(0)
                            .getBackend()
                            .getService()
                            .getName());
        } else {
            io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress ingressV1beta1 =
                    getIngressV1beta1(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE);
            assertNotNull(ingressV1beta1);
            assertEquals(
                    expectedServiceName,
                    ingressV1beta1
                            .getSpec()
                            .getRules()
                            .get(0)
                            .getHttp()
                            .getPaths()
                            .get(0)
                            .getBackend()
                            .getServiceName());
        }
    }

    private void assertIngressDoesNotExist() {
        if (IngressUtils.ingressInNetworkingV1(kubernetesClient)) {
            assertNull(getIngressV1(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE));
        } else {
            assertNull(getIngressV1beta1(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE));
        }
    }

    private Ingress getIngressV1(String name, String namespace) {
        return kubernetesClient
                .network()
                .v1()
                .ingresses()
                .inNamespace(namespace)
                .withName(name)
                .get();
    }

    private io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress getIngressV1beta1(
            String name, String namespace) {
        return kubernetesClient
                .network()
                .v1beta1()
                .ingresses()
                .inNamespace(namespace)
                .withName(name)
                .get();
    }

    // ==================== Ingress Rotation Tests ====================

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyIngressSwitchesDuringTransition(FlinkVersion flinkVersion) throws Exception {
        // Build deployment with ingress spec
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.STATELESS);

        IngressSpec ingressSpec =
                IngressSpec.builder()
                        .template("{{name}}.{{namespace}}.example.com")
                        .className("nginx")
                        .annotations(Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/"))
                        .build();
        blueGreenDeployment.getSpec().setIngress(ingressSpec);

        // 1. Deploy Blue with ingress
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // 2. Verify ingress points to Blue
        assertIngressPointsToService(BLUE_CLUSTER_ID + REST_SVC_NAME_SUFFIX);

        // 3. Trigger transition to Green
        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue, ALT_DELETION_DELAY_VALUE, null);

        // Transition to Green
        testTransitionToGreen(rs, customValue, null);

        // 4. Verify ingress now points to Green
        assertIngressPointsToService(GREEN_CLUSTER_ID + REST_SVC_NAME_SUFFIX);
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyIngressCreatedOnlyWhenConfigured(FlinkVersion flinkVersion) throws Exception {
        // 1. Deploy Blue without ingress spec
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.STATELESS);

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // 2. Verify no ingress created initially
        assertIngressDoesNotExist();

        // 3. Add ingress spec and trigger transition to Green
        IngressSpec ingressSpec =
                IngressSpec.builder()
                        .template("{{name}}.{{namespace}}.example.com")
                        .className("nginx")
                        .build();
        rs.deployment.getSpec().setIngress(ingressSpec);
        kubernetesClient.resource(rs.deployment).createOrReplace();

        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue, ALT_DELETION_DELAY_VALUE, null);

        // Complete transition to Green (this reconciles ingress)
        testTransitionToGreen(rs, customValue, null);

        // 4. Verify ingress created and points to Green after transition completes
        assertIngressPointsToService(GREEN_CLUSTER_ID + REST_SVC_NAME_SUFFIX);
    }

    // ==================== Cutover source-drain gate ====================

    /**
     * With the drain gate enabled, the previous deployment must be kept alive while the new
     * deployment still reports pending records, and deleted once the backlog is drained.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyCutoverWaitsForSourceDrain(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(CUTOVER_DRAIN_TIMEOUT.key(), "60000");
        configuration.put(DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // Green reports a backlog first, then drains on the following check
        flinkService.setPendingRecords(500L, 0L, 0L, 0L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState());

        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        // Marks Green ready and arms the (zero) deletion delay
        rs = reconcile(rs.deployment);
        // Drain check #1: 500 pending -> Blue must be kept
        rs = reconcile(rs.deployment);
        assertEquals(
                2,
                getFlinkDeployments().size(),
                "the previous deployment must be kept while the new one has pending records");
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "the transition must not finalize before the backlog is drained");

        // Drain check #2: 0 pending, and past the settle window -> cutover proceeds
        Thread.sleep(300);
        rs = reconcile(rs.deployment);
        assertEquals(
                1,
                getFlinkDeployments().size(),
                "the previous deployment must be deleted once the backlog is drained");
        // On this release line the delete call does not finalize; the next reconcile does
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState());
    }

    /**
     * With the drain gate enabled, a backlog that does not drain within the drain timeout must
     * abort the transition: the new deployment is suspended and the previous one stays active.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyCutoverAbortsWhenSourceDrainTimesOut(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(CUTOVER_DRAIN_TIMEOUT.key(), "500");
        configuration.put(DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // Green never drains
        flinkService.setPendingRecords(1_000L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        var abortRef = new AtomicReference<>(rs);
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            abortRef.set(reconcile(abortRef.get().deployment));
                            assertEquals(
                                    FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                                    abortRef.get().reconciledStatus.getBlueGreenState());
                        });
        rs = abortRef.get();

        assertFailingJobStatus(rs);
        // An abort ends the transition, so it must clear the markers that scope it. The ready
        // timestamp is the origin of the hold clock, the settle window and the drain timeout, so
        // leaving the aborted attempt's value behind pre-expires all three on the next transition:
        // it would start with a stale clock, treat an absent metric as idle rather than unknown,
        // and cut over before a single sample had been read.
        assertEquals(
                millisToInstantStr(0),
                rs.reconciledStatus.getDeploymentReadyTimestamp(),
                "an aborted transition must not leak its ready timestamp to the next one");
        assertEquals(
                millisToInstantStr(0),
                rs.reconciledStatus.getAbortTimestamp(),
                "an aborted transition must not leak its abort timestamp to the next one");
        assertEquals(
                2, getFlinkDeployments().size(), "the failed deployment is kept for inspection");
        assertEquals(
                JobState.RUNNING,
                getFlinkDeployments().get(0).getSpec().getJob().getState(),
                "the previous deployment must keep running after a drain-timeout abort");
        assertEquals(
                JobState.SUSPENDED,
                getFlinkDeployments().get(1).getSpec().getJob().getState(),
                "the new deployment must be suspended on a drain-timeout abort");
    }

    /** With the drain gate disabled (default), pending records must not delay the cutover. */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyCutoverIgnoresSourceDrainWhenDisabled(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // A large backlog that would block the gate if it were enabled
        flinkService.setPendingRecords(1_000_000L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);

        assertEquals(
                1,
                getFlinkDeployments().size(),
                "with the gate disabled the previous deployment is deleted regardless of backlog");
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState());
    }

    /**
     * Regression test for a premature cutover observed on a real cluster: task metrics reach the
     * JobManager on the heartbeat interval, so a freshly started job reports no pendingRecords for
     * a while even though its sources are already consuming. That silence must not be read as an
     * empty backlog, otherwise the previous deployment is deleted while the new one is still
     * catching up.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyUnreportedPendingRecordsHoldsUntilIdleIsConfirmed(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(CUTOVER_DRAIN_TIMEOUT.key(), "60000");
        configuration.put(DEPLOYMENT_DELETION_DELAY.key(), "0");
        // a long reschedule interval keeps the checks inside the settle window
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "5000");
        configuration.put(ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // No source reports the metric at all - the state the real cluster was in right after the
        // child became ready
        flinkService.setPendingRecordsUnreported();

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);

        assertEquals(
                2,
                getFlinkDeployments().size(),
                "an unreported metric must not be read as idle inside the settle window");
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "the transition must not finalize while the backlog is unknown");
    }

    /**
     * A source reading an idle topic never exposes pendingRecords at all, so silence that persists
     * past the settle window must be read as an empty backlog - otherwise a job with any
     * permanently idle source could never cut over and every transition would end in an abort.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyUnreportedPendingRecordsCutsOverOnceIdleIsConfirmed(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(CUTOVER_DRAIN_TIMEOUT.key(), "60000");
        configuration.put(DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        flinkService.setPendingRecordsUnreported();

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        // marks the new deployment ready, then holds while still inside the settle window
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(
                2,
                getFlinkDeployments().size(),
                "the previous deployment must be kept until the silence is confirmed");

        // past the settle window the continued silence is read as an idle source
        Thread.sleep(300);
        rs = reconcile(rs.deployment);
        assertEquals(
                1,
                getFlinkDeployments().size(),
                "a source that never exposes the metric must not block the cutover forever");
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState());
    }

    /**
     * A transition that follows an aborted one must start its hold clock from zero. The ready
     * timestamp is only ever stamped when it reads zero, so a value left behind by the abort is
     * never replaced - the next transition would compute its settle window and its drain deadline
     * from the previous attempt's clock and promote before observing anything.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyAbortDoesNotDisarmTheFollowingTransition(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(CUTOVER_DRAIN_TIMEOUT.key(), "1");
        configuration.put(DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "5000");
        configuration.put(ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // Transition 1: a backlog that never drains, with a timeout that expires at once
        flinkService.setPendingRecords(5000L);
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        var abortRef = new AtomicReference<>(rs);
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            abortRef.set(reconcile(abortRef.get().deployment));
                            assertEquals(
                                    FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                                    abortRef.get().reconciledStatus.getBlueGreenState());
                        });
        rs = abortRef.get();

        // Transition 2: any later transition - the deliberate redeploy after fixing the cause, or
        // an unrelated change days on. Give it a real budget, and let the metric be absent: the
        // state a job is in before its first record, when its backlog is largest.
        // Nothing retries on its own - a person diagnoses the abort and pushes a new spec. Any
        // gap longer than the settle window is enough for an inherited clock to read as settled,
        // and the leak is not scoped to a deliberate retry: it poisons every later transition.
        Thread.sleep(2 * 5000 + 1000);

        rs.deployment.getSpec().getConfiguration().put(CUTOVER_DRAIN_TIMEOUT.key(), "600000");
        flinkService.setPendingRecordsUnreported();

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);

        assertEquals(
                2,
                getFlinkDeployments().size(),
                "a transition following an abort must hold: with a fresh clock an absent metric"
                        + " inside the settle window is unknown, not idle");
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "a later transition must not finalize on an inherited clock");
    }

    /**
     * An expired drain timeout must abort even if the backlog reads drained on the same evaluation.
     * Checking the deadline after the backlog made the timeout advisory rather than binding: a zero
     * arriving at expiry won, which is precisely the reading least worth trusting there, and a lost
     * status update re-opened the gate for a later reconcile to promote on.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyExpiredDeadlineAbortsEvenWhenTheBacklogReadsDrained(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        // already expired by the time the first verdict is taken
        configuration.put(CUTOVER_DRAIN_TIMEOUT.key(), "1");
        configuration.put(DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // a drained backlog, settled - everything the cutover path wants
        flinkService.setPendingRecords(0L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);
        Thread.sleep(300);
        rs = reconcile(rs.deployment);

        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "an expired deadline must abort, not cut over on a zero read at expiry");
        assertEquals(
                2,
                getFlinkDeployments().size(),
                "the outgoing deployment must survive an expired deadline");
    }

    /** A zero read on the very first check after readiness must not be enough to cut over. */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyFirstZeroReadingIsNotEnoughToCutOver(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(CUTOVER_DRAIN_TIMEOUT.key(), "60000");
        configuration.put(DEPLOYMENT_DELETION_DELAY.key(), "0");
        // a long reschedule interval makes the settle window observable in the test
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "5000");
        configuration.put(ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        flinkService.setPendingRecords(0L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        // marks ready, then the first drain check lands inside the settle window
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);

        assertEquals(
                2,
                getFlinkDeployments().size(),
                "a zero read inside the settle window must not trigger the cutover");
    }
}
