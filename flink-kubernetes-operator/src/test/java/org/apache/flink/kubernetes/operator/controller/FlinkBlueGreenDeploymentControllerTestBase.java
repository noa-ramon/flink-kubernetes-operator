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
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
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
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.SAMPLE_JAR;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_DEPLOYMENT_NAME;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_NAMESPACE;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_ABORT_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.instantStrToMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared harness for the {@link FlinkBlueGreenDeploymentController} test classes: the mock
 * Kubernetes client, the testing controller and service, and the helpers that drive a
 * blue/green transition end to end.
 */
@EnableKubernetesMockClient(crud = true)
public abstract class FlinkBlueGreenDeploymentControllerTestBase {

    public static final String SERVICE_ACCOUNT = "flink-operator";
    public static final String FLINK_VERSION = "latest";
    public static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    public static final String IMAGE_POLICY = "IfNotPresent";

    protected static final String CUSTOM_CONFIG_FIELD = "custom-configuration-field";
    protected static final int DEFAULT_DELETION_DELAY_VALUE = 500;
    protected static final int ALT_DELETION_DELAY_VALUE = 1000;
    protected static final int MINIMUM_ABORT_GRACE_PERIOD = 1000;
    protected static final String TEST_CHECKPOINT_PATH = "/tmp/checkpoints";
    protected static final String TEST_INITIAL_SAVEPOINT_PATH = "/tmp/savepoints";
    protected static final String BLUE_CLUSTER_ID = TEST_DEPLOYMENT_NAME + "-blue";
    protected static final String GREEN_CLUSTER_ID = TEST_DEPLOYMENT_NAME + "-green";
    protected static final String REST_SVC_NAME_SUFFIX = "-rest";
    protected final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    protected TestingFlinkService flinkService;
    protected Context<FlinkBlueGreenDeployment> context;
    protected TestingFlinkBlueGreenDeploymentController testController;

    protected KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService(kubernetesClient);
        context = flinkService.getContext();
        testController = new TestingFlinkBlueGreenDeploymentController(configManager, flinkService);
    }

    @NotNull
    protected TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult handleSavepoint(
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

    protected TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
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
    protected TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
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

    protected static FlinkBlueGreenDeployment buildSessionCluster(
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

    protected void simulateChangeInSpec(
            FlinkBlueGreenDeployment blueGreenDeployment,
            String customFieldValue,
            int customDeletionDelayMs,
            String initialSavepointPath) {
        FlinkBlueGreenDeploymentSpec bgSpec = blueGreenDeployment.getSpec();
        FlinkDeploymentTemplateSpec template = bgSpec.getTemplate();

        if (customDeletionDelayMs > 0) {
            bgSpec.getConfiguration()
                    .put(
                            BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(),
                            String.valueOf(customDeletionDelayMs));
        }

        FlinkDeploymentSpec spec = template.getSpec();
        spec.getFlinkConfiguration().put(CUSTOM_CONFIG_FIELD, customFieldValue);

        if (initialSavepointPath != null) {
            spec.getJob().setInitialSavepointPath(initialSavepointPath);
        }

        template.setSpec(spec);
        kubernetesClient.resource(blueGreenDeployment).createOrReplace();
    }

    protected void simulateSubmitAndSuccessfulJobStart(FlinkDeployment deployment) throws Exception {
        // TODO: is this correct? Doing this to give the TestingFlinkService awareness of the job
        JobSpec jobSpec = deployment.getSpec().getJob();
        Configuration conf = new Configuration();
        conf.set(SavepointConfigOptions.SAVEPOINT_PATH, TEST_CHECKPOINT_PATH);
        flinkService.submitApplicationCluster(jobSpec, conf, false);
        var jobId = flinkService.listJobs().get(0).f1.getJobId().toString();
        deployment.getStatus().getJobStatus().setJobId(jobId);
        simulateSuccessfulJobStart(deployment);
    }

    protected void simulateSuccessfulJobStart(FlinkDeployment deployment) {
        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        deployment.getStatus().getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        deployment.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
        kubernetesClient.resource(deployment).update();
    }

    /*
    Convenience function to reconcile and get the frequently used `BlueGreenReconciliationResult`
     */
    protected TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult reconcile(
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

    protected List<FlinkDeployment> getFlinkDeployments() {
        return kubernetesClient
                .resources(FlinkDeployment.class)
                .inNamespace(TEST_NAMESPACE)
                .list()
                .getItems();
    }

    protected static void assertFailingJobStatus(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs) {
        assertEquals(JobStatus.FAILING, rs.reconciledStatus.getJobStatus().getState());
        assertNotNull(rs.reconciledStatus.getError());
    }

    /**
     * A mutable {@link Clock} the gate tests drive explicitly. Every read ticks it forward by a
     * millisecond, the way a wall clock never reads the same instant twice, and the window and
     * deadline crossings are driven by {@link #advance} instead of sleeping against real time.
     */
    protected static final class SteppingClock extends Clock {
        private long millis = System.currentTimeMillis();

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            millis += 1;
            return Instant.ofEpochMilli(millis);
        }

        void advance(long millisToAdd) {
            millis += millisToAdd;
        }
    }

    protected SteppingClock useSteppingClock() {
        var clock = new SteppingClock();
        testController
                .flinkBlueGreenDeploymentController
                .getHandlerRegistry()
                .getDeploymentService()
                .setClock(clock);
        return clock;
    }

    protected static void verifyOwnerReferences(
            FlinkBlueGreenDeployment parent, FlinkDeployment child) {
        var ownerReferences = child.getMetadata().getOwnerReferences();
        assertEquals(1, ownerReferences.size());
        var ownerRef = ownerReferences.get(0);
        assertEquals(parent.getMetadata().getName(), ownerRef.getName());
        assertEquals(parent.getKind(), ownerRef.getKind());
        assertEquals(parent.getApiVersion(), ownerRef.getApiVersion());
    }

    protected void assertFinalized(
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

    protected static FlinkBlueGreenDeploymentSpec getTestFlinkDeploymentSpec(FlinkVersion version) {
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
                        .jobManager(
                                JobManagerSpec.builder()
                                        .resource(new Resource(1.0, "2048m", "2G"))
                                        .replicas(1)
                                        .build())
                        .taskManager(
                                TaskManagerSpec.builder()
                                        .resource(new Resource(1.0, "2048m", "2G"))
                                        .build())
                        .build();

        flinkDeploymentSpec.setFlinkConfiguration(conf);

        Map<String, String> configuration = new HashMap<>();
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "1");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "500");
        configuration.put(
                BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(),
                String.valueOf(DEFAULT_DELETION_DELAY_VALUE));

        var flinkDeploymentTemplateSpec =
                FlinkDeploymentTemplateSpec.builder().spec(flinkDeploymentSpec).build();

        return new FlinkBlueGreenDeploymentSpec(configuration, null, flinkDeploymentTemplateSpec);
    }

}
