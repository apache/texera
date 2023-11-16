package edu.uci.ics.amber.engine.common

import akka.actor.Address

import scala.concurrent.duration._

object Constants {
  // time interval for logging queue sizes
  val loggingQueueSizeInterval: Int =
    AmberUtils.amberConfig.getInt("constants.logging-queue-size-interval")
  val MAX_RESOLUTION_ROWS: Int = AmberUtils.amberConfig.getInt("constants.max-resolution-rows")
  val MAX_RESOLUTION_COLUMNS: Int =
    AmberUtils.amberConfig.getInt("constants.max-resolution-columns")

  // Non constants: TODO: move out from Constants
  var numWorkerPerNode: Int = AmberUtils.amberConfig.getInt("constants.num-worker-per-node")
  var currentWorkerNum = 0
  var masterNodeAddr: Address = Address("akka", "Amber", "localhost", 2552)
  var defaultTau: FiniteDuration = 10.milliseconds

  // monitoring and reshape related
  var monitoringEnabled: Boolean =
    AmberUtils.amberConfig.getBoolean("monitoring.monitoring-enabled")
  var monitoringIntervalInMs: Int =
    AmberUtils.amberConfig.getInt("monitoring.monitoring-interval-ms")
  var reshapeSkewHandlingEnabled: Boolean =
    AmberUtils.amberConfig.getBoolean("reshape.skew-handling-enabled")
  var reshapeSkewDetectionInitialDelayInMs: Int =
    AmberUtils.amberConfig.getInt("reshape.skew-detection-initial-delay-ms")
  var reshapeSkewDetectionIntervalInMs: Int =
    AmberUtils.amberConfig.getInt("reshape.skew-detection-interval-ms")
  var reshapeEtaThreshold: Int =
    AmberUtils.amberConfig.getInt("reshape.eta-threshold")
  var reshapeTauThreshold: Int =
    AmberUtils.amberConfig.getInt("reshape.tau-threshold")
  var reshapeHelperOverloadThreshold: Int =
    AmberUtils.amberConfig.getInt("reshape.helper-overload-threshold")
  var reshapeMaxWorkloadSamplesInController: Int =
    AmberUtils.amberConfig.getInt("reshape.max-workload-samples-controller")
  var reshapeMaxWorkloadSamplesInWorker: Int =
    AmberUtils.amberConfig.getInt("reshape.max-workload-samples-worker")
  var reshapeWorkloadSampleSize: Int =
    AmberUtils.amberConfig.getInt("reshape.workload-sample-size")
  var reshapeFirstPhaseSharingNumerator: Int =
    AmberUtils.amberConfig.getInt("reshape.first-phase-sharing-numerator")
  var reshapeFirstPhaseSharingDenominator: Int =
    AmberUtils.amberConfig.getInt("reshape.first-phase-sharing-denominator")

  // flow control related
  var flowControlEnabled: Boolean =
    AmberUtils.amberConfig.getBoolean("flow-control.credit-based-flow-control-enabled")
  var unprocessedBatchesSizeLimitInBytesPerWorkerPair: Int =
    AmberUtils.amberConfig.getInt(
      "flow-control.unprocessed-batches-size-limit-in-bytes-per-worker-pair"
    )
  var creditPollingInitialDelayInMs: Int =
    AmberUtils.amberConfig.getInt("flow-control.credit-poll-request-delay-in-ms")

  var schedulingPolicyName: String = AmberUtils.amberConfig.getString("scheduling.policy-name")
  var timeSlotExpirationDurationInMs: Int =
    AmberUtils.amberConfig.getInt("scheduling.time-slot-expiration-duration-ms")

  var enableTransactionalReconfiguration: Boolean =
    AmberUtils.amberConfig.getBoolean("reconfiguration.enable-transactional-reconfiguration")

  val defaultBatchSize: Int = AmberUtils.amberConfig.getInt("network-buffering.default-batch-size")
  var enableAdaptiveNetworkBuffering: Boolean =
    AmberUtils.amberConfig.getBoolean("network-buffering.enable-adaptive-buffering")
  var adaptiveBufferingTimeoutMs: Int =
    AmberUtils.amberConfig.getInt("network-buffering.adaptive-buffering-timeout-ms")
}
