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

package org.apache.flink.kubernetes.operator.api.spec;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Configuration options to be used by the Flink Blue/Green Deployments. */
public class FlinkBlueGreenDeploymentConfigOptions {

    public static final String K8S_OP_CONF_PREFIX = "kubernetes.operator.";

    public static final String BLUE_GREEN_CONF_PREFIX = K8S_OP_CONF_PREFIX + "bluegreen.";

    public static ConfigOptions.OptionBuilder operatorConfig(String key) {
        return ConfigOptions.key(BLUE_GREEN_CONF_PREFIX + key);
    }

    /**
     * NOTE: The string durations need to be in format "{time unit value}{time unit label}", e.g.
     * "123ms", "321 s". If no time unit label is specified, it will be considered as milliseconds.
     * There is no fall back to parse ISO-8601 duration format, until Flink 2.x
     *
     * <p>Supported time unit labels are:
     *
     * <ul>
     *   <li>DAYS： "d", "day"
     *   <li>HOURS： "h", "hour"
     *   <li>MINUTES： "m", "min", "minute"
     *   <li>SECONDS： "s", "sec", "second"
     *   <li>MILLISECONDS： "ms", "milli", "millisecond"
     *   <li>MICROSECONDS： "us", "micro", "microsecond"
     *   <li>NANOSECONDS： "ns", "nano", "nanosecond"
     * </ul>
     */
    public static final ConfigOption<Duration> ABORT_GRACE_PERIOD =
            operatorConfig("abort.grace-period")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The max time to wait for a deployment to become ready before aborting it.");

    public static final ConfigOption<Duration> RECONCILIATION_RESCHEDULING_INTERVAL =
            operatorConfig("reconciliation.reschedule-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(15))
                    .withDescription(
                            "Configurable delay to use when the operator reschedules a reconciliation.");

    public static final ConfigOption<Duration> DEPLOYMENT_DELETION_DELAY =
            operatorConfig("deployment-deletion.delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "Configurable delay before deleting a deployment after being marked done.");
    public static final ConfigOption<Boolean> CUTOVER_WAIT_FOR_SOURCE_DRAIN =
            operatorConfig("cutover.wait-for-source-drain")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If enabled, the previous deployment is kept running until the new deployment has"
                                    + " drained its source backlog (no pending records), instead of being deleted as"
                                    + " soon as the new deployment is ready. A deployment restored from a savepoint is"
                                    + " ready while it still replays the records produced during its own startup; for"
                                    + " event time pipelines with a bounded allowed lateness, deleting the previous"
                                    + " deployment during that catch-up leaves those records without a producer."
                                    + " Requires sources exposing the pendingRecords metric (e.g. KafkaSource);"
                                    + " a job where no source reports it never confirms as drained and the"
                                    + " transition is aborted once cutover.drain-timeout expires.");

    public static final ConfigOption<Duration> CUTOVER_DRAIN_TIMEOUT =
            operatorConfig("cutover.drain-timeout")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            "The max time to wait for the new deployment to drain its source backlog when"
                                    + " cutover.wait-for-source-drain is enabled, measured from the moment the gate"
                                    + " starts evaluating: when the new deployment became ready plus any"
                                    + " deployment-deletion.delay. If exceeded, the transition is aborted: the new"
                                    + " deployment is suspended and the previous one remains active.");

    public static final ConfigOption<Duration> CUTOVER_DRAIN_CONFIRMATION_WINDOW =
            operatorConfig("cutover.drain-confirmation-window")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "How long the new deployment's backlog must be read as continuously empty before the"
                                    + " cutover proceeds when cutover.wait-for-source-drain is enabled. The window is"
                                    + " measured from the first zero reading of the current incarnation of the job -"
                                    + " never from wall time alone: a single reading is not evidence, since sources"
                                    + " initialise pendingRecords lazily and a restart wipes the gauge. The window"
                                    + " restarts whenever a non-zero backlog is read, no source reports the metric,"
                                    + " the job or one of its source vertices restarts, or the set of reporting"
                                    + " sources changes.");
}
