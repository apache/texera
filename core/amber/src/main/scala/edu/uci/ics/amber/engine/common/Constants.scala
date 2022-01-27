package edu.uci.ics.amber.engine.common

import scala.concurrent.duration._

object Constants {
  val defaultBatchSize: Int = AmberUtils.amberConfig.getInt("constants.default-batch-size")
  // time interval for logging queue sizes
  val loggingQueueSizeInterval: Int =
    AmberUtils.amberConfig.getInt("constants.logging-queue-size-interval")

  // Non constants: TODO: move out from Constants
  var numWorkerPerNode: Int = AmberUtils.amberConfig.getInt("constants.num-worker-per-node")
  var dataVolumePerNode: Int = AmberUtils.amberConfig.getInt("constants.data-volume-per-node")
  var currentWorkerNum = 0
  var currentDataSetNum = 0
  var masterNodeAddr: Option[String] = None
  var defaultTau: FiniteDuration = 10.milliseconds

  var monitoringIntervalInMs: Int =
    AmberUtils.amberConfig.getInt("monitoring.monitoring-interval-ms")
  var reshapeSkewHandlingEnabled: Boolean =
    AmberUtils.amberConfig.getBoolean("reshape.skew-handling-enabled")
  var skewDetectionIntervalInMs: Int =
    AmberUtils.amberConfig.getInt("reshape.skew-detection-interval-ms")
  var reshapeEtaThreshold: Int =
    AmberUtils.amberConfig.getInt("reshape.eta-threshold")
  var reshapeTauThreshold: Int =
    AmberUtils.amberConfig.getInt("reshape.tau-threshold")
  var reshapeHelperOverloadThreshold: Int =
    AmberUtils.amberConfig.getInt("reshape.helper-overload-threshold")
  var reshapeFirstPhaseSharingNumerator: Int =
    AmberUtils.amberConfig.getInt("reshape.first-phase-sharing-numerator")
  var reshapeFirstPhaseSharingDenominator: Int =
    AmberUtils.amberConfig.getInt("reshape.first-phase-sharing-denominator")
}
