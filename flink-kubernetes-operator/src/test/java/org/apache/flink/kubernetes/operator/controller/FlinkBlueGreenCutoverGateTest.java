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
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_DEPLOYMENT_NAME;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_NAMESPACE;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_ABORT_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_CUTOVER_DRAIN_TIMEOUT;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.millisToInstantStr;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for the blue/green cutover drain gate: the observation-anchored confirmation window, the
 * fail-closed drain deadline, and the readiness latch that keeps a mid-hold dip from being judged
 * by the expired become-ready deadline.
 */
public class FlinkBlueGreenCutoverGateTest extends FlinkBlueGreenDeploymentControllerTestBase {

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
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "60000");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "300");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

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

        // Drain check #2: the first zero reading starts the confirmation window - it must hold
        rs = reconcile(rs.deployment);
        assertEquals(2, getFlinkDeployments().size(), "one zero reading must not cut over");
        // Drain check #3: still inside the window
        rs = reconcile(rs.deployment);
        assertEquals(2, getFlinkDeployments().size(), "a zero inside the window must not cut over");
        // Drain check #4: the backlog has read empty across the whole window
        clock.advance(350);
        rs = reconcile(rs.deployment);
        assertEquals(
                1,
                getFlinkDeployments().size(),
                "the previous deployment must be deleted once the backlog has read empty across"
                        + " the confirmation window");
        // The successful delete finalizes; one more reconcile settles the status
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
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "500");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        // Green never drains
        flinkService.setPendingRecords(1_000L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        clock.advance(1000);
        for (int i = 0;
                i < 5
                        && rs.reconciledStatus.getBlueGreenState()
                                != FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
                i++) {
            rs = reconcile(rs.deployment);
        }
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "the drain deadline must abort the transition");

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
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

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
    public void verifyUnreportedPendingRecordsAlwaysHold(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "60000");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        // a long reschedule interval keeps the checks inside the settle window
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "5000");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

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
                "an unreported metric must always hold - silence is never evidence of a drain");
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "the transition must not finalize while the backlog is unknown");
    }

    /**
     * A job whose sources never expose pendingRecords cannot prove its backlog is drained, and
     * silence is not evidence: FLIP-33 marks the metric optional and a restart wipes the gauge, so
     * absence is indistinguishable from metric loss over a real backlog. Such a transition fails
     * closed at the drain timeout - the previous deployment keeps running.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyAllSilentJobAbortsAtDrainTimeout(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "200");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        flinkService.setPendingRecordsUnreported();

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        clock.advance(1000);
        for (int i = 0;
                i < 5
                        && rs.reconciledStatus.getBlueGreenState()
                                != FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
                i++) {
            rs = reconcile(rs.deployment);
        }
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "the drain deadline must abort the transition");

        assertFailingJobStatus(rs);
        assertEquals(
                2,
                getFlinkDeployments().size(),
                "silence must never promote: the previous deployment survives the abort");
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
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "1");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "5000");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        // Transition 1: a backlog that never drains, with a timeout that expires at once
        flinkService.setPendingRecords(5000L);
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        clock.advance(1000);
        for (int i = 0;
                i < 5
                        && rs.reconciledStatus.getBlueGreenState()
                                != FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
                i++) {
            rs = reconcile(rs.deployment);
        }
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "the drain deadline must abort the transition");

        // Transition 2: any later transition - the deliberate redeploy after fixing the cause, or
        // an unrelated change days on. Give it a real budget, and let the metric be absent: the
        // state a job is in before its first record, when its backlog is largest.
        // Nothing retries on its own - a person diagnoses the abort and pushes a new spec. Any
        // gap longer than the settle window is enough for an inherited clock to read as settled,
        // and the leak is not scoped to a deliberate retry: it poisons every later transition.
        clock.advance(2 * 5000 + 1000);

        rs.deployment
                .getSpec()
                .getConfiguration()
                .put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "600000");
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
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        // already expired by the time the first verdict is taken, while the zero-length
        // confirmation window makes the same evaluation eligible for cutover - the abort
        // below therefore proves the deadline is checked first, not that the backlog was
        // simply unconfirmed
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "1");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "0");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        // a drained backlog, settled - everything the cutover path wants
        flinkService.setPendingRecords(0L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);
        clock.advance(300);
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

    /**
     * The read-as-idle rung exists for sources that never initialise the metric because their topic
     * is idle - without it, such a job could never cut over at all. This test constructs the case
     * where the same inference is provably wrong: the gate has already observed a large backlog,
     * then the metric disappears (a TaskManager restart wipes the gauge, which is only re-created
     * lazily on the next record). The job is not idle - the gate has first-hand evidence of 43,471
     * pending records and has never once observed a zero - yet the silence plus elapsed wall time
     * satisfies the clock-based settle and promotes, deleting the only deployment that was still
     * covering the backlog.
     *
     * <p>Idleness is a claim about the job; the settle window only measures the clock. Silence is
     * evidence of idleness only when there is no contrary observation - here there is one, and it
     * is the last numeric value the gate ever saw.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySilenceAfterAnObservedBacklogMustNotCutOver(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "600000");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        // The incoming job reports a large real backlog on the first evaluation...
        flinkService.setPendingRecords(43471L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(2, getFlinkDeployments().size(), "the observed backlog must hold the cutover");

        // ...then its TaskManagers restart: the gauge is wiped and, being lazily initialised,
        // stays absent until the next record is read. No zero has ever been observed.
        flinkService.setPendingRecordsUnreported();
        clock.advance(300); // wall time alone satisfies the clock-based settle window

        rs = reconcile(rs.deployment);

        assertEquals(
                2,
                getFlinkDeployments().size(),
                "silence directly after an observed backlog of 43,471 is metric loss, not"
                        + " idleness - the outgoing deployment must be kept");
    }

    /** An unreadable metric is not a drained backlog: a REST failure must hold, never promote. */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyUnreadableMetricHoldsTheCutover(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "600000");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "300");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        flinkService.setPendingRecordsThrows(true);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);
        // well past the settle window: elapsed time alone must never turn a failure into a drain
        clock.advance(300);
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);

        assertEquals(
                2,
                getFlinkDeployments().size(),
                "a REST failure reading the backlog must hold the previous deployment");
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "the transition must not finalize while the backlog cannot be read");

        // and once the metric is readable again, the gate resumes normally: a zero starts the
        // confirmation window, and the cutover proceeds once the window has passed
        flinkService.setPendingRecordsThrows(false);
        flinkService.setPendingRecords(0L);
        rs = reconcile(rs.deployment);
        clock.advance(350);
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "recovering the metric must let the drained cutover proceed");
    }

    /**
     * The confirmation window is anchored to the job incarnation that produced its readings. A
     * restart wipes the lazily-created gauge and replays state, so readings taken before it say
     * nothing about the job now running - the window starts over and must pass again.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyJobRestartResetsTheConfirmationWindow(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "600000");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "300");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        flinkService.setPendingRecords(0L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        // the first zero starts the window, and most of it passes...
        rs = reconcile(rs.deployment);
        assertEquals(2, getFlinkDeployments().size(), "the window has only just started");
        clock.advance(350);

        // ...then the incoming job restarts: the readings the window was built on came from a
        // gauge that no longer exists
        flinkService.bumpJobIncarnation();

        rs = reconcile(rs.deployment);
        assertEquals(
                2,
                getFlinkDeployments().size(),
                "a reading from a new incarnation must restart the window, not complete it");
        clock.advance(350);
        rs = reconcile(rs.deployment);
        assertEquals(
                1,
                getFlinkDeployments().size(),
                "a full window of readings from the same incarnation completes the drain");
    }

    /**
     * The savepoint trigger id is a baton: SAVEPOINTING stores it and returns to ACTIVE, where the
     * still-unreconciled diff consumes it. If the spec reverts in that gap - an external controller
     * undoing the change - the diff reads IGNORE and the baton is orphaned. It must be cleared then
     * and there: left in place it outlives the JobManager's completed-operation cache, and the next
     * real transition, possibly days later, fetches a trigger the JobManager no longer knows and
     * fails before it starts. Observed live: a trigger from 2026-08-28 failed the transition of
     * 2026-08-30.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyRevertedTransitionDoesNotOrphanTheSavepointTrigger(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);
        blueGreenDeployment
                .getSpec()
                .getConfiguration()
                .put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        // A transition starts and its savepoint completes...
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        var triggers = flinkService.getSavepointTriggers();
        triggers.clear();
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE,
                rs.reconciledStatus.getBlueGreenState());
        String triggerId = rs.deployment.getStatus().getSavepointTriggerId();
        triggers.put(triggerId, true);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "a completed savepoint hands the transition back to the active state");

        // ...but the spec reverts before the savepoint is consumed (an external controller
        // restoring the previous state removes the custom field entirely)
        rs.deployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getFlinkConfiguration()
                .remove(CUSTOM_CONFIG_FIELD);
        rs = reconcile(rs.deployment);

        assertNull(
                rs.reconciledStatus.getSavepointTriggerId(),
                "a reverted transition must not orphan its savepoint trigger id");
        assertEquals(1, getFlinkDeployments().size(), "no second deployment was created");

        // and the next real transition must start cleanly on a fresh savepoint
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        assertEquals(
                2,
                getFlinkDeployments().size(),
                "the next transition must create its child from a fresh savepoint");
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        for (int i = 0;
                i < 6
                        && rs.reconciledStatus.getBlueGreenState()
                                != FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
                i++) {
            rs = reconcile(rs.deployment);
        }
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "the transition after a reverted one must complete normally");
    }

    /**
     * The window claims the backlog read as continuously empty. A stretch in which the metric could
     * not be read at all is a gap in that claim, so reads resuming after a failure must start the
     * window over rather than complete it across the blind stretch.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyUnreadableStretchRestartsTheWindow(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "600000");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "300");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        flinkService.setPendingRecords(0L);
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        // the window opens on a real zero, then the metric becomes unreadable across it
        rs = reconcile(rs.deployment);
        flinkService.setPendingRecordsThrows(true);
        clock.advance(350);
        rs = reconcile(rs.deployment);
        assertEquals(2, getFlinkDeployments().size(), "an unreadable stretch must hold");

        // reads resume: wall time alone spans the window, but the gap broke the claim
        flinkService.setPendingRecordsThrows(false);
        rs = reconcile(rs.deployment);
        assertEquals(
                2,
                getFlinkDeployments().size(),
                "a zero following a blind gap must restart the window, not complete it");

        clock.advance(350);
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "a full gap-free window completes the drain");
    }

    /**
     * A zero reading taken while the job has no RUNNING timestamp - job details fetched mid-restart
     * - is a stale aggregate, not evidence: it must neither anchor nor extend the window.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyZeroWithoutRunningTimestampDoesNotCount(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "600000");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "300");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        flinkService.setPendingRecords(0L);
        flinkService.setJobRunningTimestamp(0L);
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        // zeros with no RUNNING timestamp, spanning far more wall time than the window
        rs = reconcile(rs.deployment);
        clock.advance(350);
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(
                2,
                getFlinkDeployments().size(),
                "zeros from a job that is not observably running must never complete the window");

        // the job becomes observably running: a fresh gap-free window is still required
        flinkService.setJobRunningTimestamp(7L);
        rs = reconcile(rs.deployment);
        assertEquals(2, getFlinkDeployments().size(), "the first countable zero only opens it");
        clock.advance(350);
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "a full window of countable zeros completes the drain");
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
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "60000");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        // a long reschedule interval makes the settle window observable in the test
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "5000");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

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

    /**
     * The gate is only entered after the deployment deletion delay, so the drain deadline is
     * anchored there. Anchored at readiness, a delay longer than the drain timeout would expire the
     * deadline before the first backlog reading is ever taken and abort a healthy transition on
     * entry.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyDeletionDelayDoesNotConsumeTheDrainTimeout(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        // the delay alone outlasts the drain timeout: anchored at readiness, the deadline
        // would already be expired at the gate's first evaluation
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "500");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "300");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "2000");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        flinkService.setPendingRecords(0L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        // marks the new deployment ready and arms the deletion delay
        rs = reconcile(rs.deployment);
        // the delay elapses; measured from readiness the drain timeout is long expired by now
        clock.advance(2000);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "the deletion delay must not consume the drain timeout: the first evaluation"
                        + " opens the confirmation window instead of aborting");
        assertEquals(2, getFlinkDeployments().size());

        // the confirmation window passes with the deadline still open
        clock.advance(350);
        rs = reconcile(rs.deployment);
        assertEquals(1, getFlinkDeployments().size(), "the drained backlog completes the cutover");
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState());
    }

    /**
     * The abort timestamp bounds becoming ready and is armed once, at transition start. A
     * deployment holding for the drain gate is necessarily past that deadline, so when its
     * readiness dips for seconds - an in-place autoscaler rescale flips STABLE to DEPLOYED - the
     * dip must not be judged by it: observed live, a 998 ms dip aborted a healthy transition with
     * 24 minutes of drain budget unused. Past first readiness the hold continues, and the recovered
     * deployment drains and cuts over normally.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyReadinessDipDuringTheHoldDoesNotAbort(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "600000");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "300");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        // a short grace period, so the phase-one deadline is long expired during the hold
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "1000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        // a backlog that does not drain, so the gate keeps holding
        flinkService.setPendingRecords(500L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        var green = getFlinkDeployments().get(1);
        simulateSuccessfulJobStart(green);
        // marks ready, then the gate holds on the backlog
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(2, getFlinkDeployments().size());

        // deep into the hold, far past the phase-one grace period...
        clock.advance(5000);
        // ...readiness dips: an in-place rescale takes the job out of RUNNING for seconds
        green = getFlinkDeployments().get(1);
        green.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
        kubernetesClient.resource(green).update();
        rs = reconcile(rs.deployment);

        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "a readiness dip during the hold must not be judged by the expired"
                        + " become-ready deadline");
        assertEquals(
                2,
                getFlinkDeployments().size(),
                "both deployments must survive a transient dip during the hold");

        // the dip ends, the backlog drains, and the held transition completes normally
        green = getFlinkDeployments().get(1);
        green.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        kubernetesClient.resource(green).update();
        flinkService.setPendingRecords(0L);
        rs = reconcile(rs.deployment);
        clock.advance(350);
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "the recovered deployment must drain and cut over normally");
    }

    /**
     * The latch must not convert the abort into a wedge: a deployment that goes unready and stays
     * unready is no longer a transient dip, and it is bounded by the deadline that governs the hold
     * - the drain deadline - rather than held forever.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyUnreadyPastTheDrainDeadlineAborts(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "2000");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "300");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "1000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        flinkService.setPendingRecords(500L);

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        var green = getFlinkDeployments().get(1);
        simulateSuccessfulJobStart(green);
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);

        // the deployment goes unready and never recovers
        green = getFlinkDeployments().get(1);
        green.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
        kubernetesClient.resource(green).update();
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "inside the drain budget an unready hold is still a hold");

        // past the drain deadline the hold is no longer transient
        clock.advance(3000);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "a deployment unready past the drain deadline must abort, not wedge");
        assertEquals(
                2, getFlinkDeployments().size(), "the failed deployment is kept for inspection");
    }

    /**
     * With the drain gate off the latch must not engage: a deployment that goes unready after
     * readiness, past the grace period, aborts exactly as stock does today.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyDipAfterReadinessStillAbortsWithTheGateOff(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        // gate off; a deletion delay long enough that the dip happens mid-wait
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "60000");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "1000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        var green = getFlinkDeployments().get(1);
        simulateSuccessfulJobStart(green);
        // marks ready, arms the deletion delay
        rs = reconcile(rs.deployment);

        clock.advance(5000);
        green = getFlinkDeployments().get(1);
        green.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
        kubernetesClient.resource(green).update();
        rs = reconcile(rs.deployment);

        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                rs.reconciledStatus.getBlueGreenState(),
                "with the gate off, behaviour is unchanged: an expired grace period aborts");
    }

    /**
     * A re-spec while the gate is holding replaces the candidate mid-flight. The readiness stamp
     * guards on zero and is never re-taken, so without a reset the rebuilt candidate inherits the
     * previous candidate's clock and its drain deadline expires early - observed live as an abort
     * with 898k records still pending and 17 minutes of honest budget remaining. The re-spec must
     * reset the transition markers so the rebuilt candidate re-arms a full drain budget of its own.
     */
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyReSpecDuringTheHoldGrantsAFreshDrainBudget(FlinkVersion flinkVersion)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.SAVEPOINT);

        Map<String, String> configuration = blueGreenDeployment.getSpec().getConfiguration();
        configuration.put(BLUEGREEN_CUTOVER_WAIT_FOR_SOURCE_DRAIN.key(), "true");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_TIMEOUT.key(), "2000");
        configuration.put(BLUEGREEN_CUTOVER_DRAIN_CONFIRMATION_WINDOW.key(), "300");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "0");
        configuration.put(BLUEGREEN_RECONCILIATION_RESCHEDULING_INTERVAL.key(), "100");
        configuration.put(BLUEGREEN_ABORT_GRACE_PERIOD.key(), "60000");

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);
        var clock = useSteppingClock();

        // transition 1: a backlog holds the gate while most of the drain budget burns down
        flinkService.setPendingRecords(500L);
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = handleSavepoint(rs);
        rs = reconcile(rs.deployment);
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(2, getFlinkDeployments().size());
        clock.advance(1500);

        // a second spec change lands mid-hold: the candidate is rebuilt with the new spec
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);
        rs = reconcile(rs.deployment);
        assertEquals(
                millisToInstantStr(0),
                rs.reconciledStatus.getDeploymentReadyTimestamp(),
                "a re-spec replaces the candidate, so the previous candidate's readiness"
                        + " clock must not survive it");

        // the rebuilt candidate boots and holds; on the inherited clock its deadline would
        // already be expired - on its own it has the full budget
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);
        clock.advance(700);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "the rebuilt candidate must be judged by its own drain budget, not the"
                        + " previous candidate's leftovers");
        assertEquals(2, getFlinkDeployments().size());

        // and with a full budget of its own, it drains and cuts over normally
        flinkService.setPendingRecords(0L);
        rs = reconcile(rs.deployment);
        clock.advance(350);
        rs = reconcile(rs.deployment);
        rs = reconcile(rs.deployment);
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState(),
                "the re-specced transition completes on its own clock");
    }
}
