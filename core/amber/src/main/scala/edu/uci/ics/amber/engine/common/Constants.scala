package edu.uci.ics.amber.engine.common

import akka.actor.Address

import scala.concurrent.duration._

object Constants {
  val defaultBatchSize: Int = AmberUtils.amberConfig.getInt("constants.default-batch-size")
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

  var monitoringEnabled: Boolean =
    AmberUtils.amberConfig.getBoolean("constants.monitoring-enabled")
  var reshapeSkewHandlingEnabled: Boolean =
    AmberUtils.amberConfig.getBoolean("constants.reshape-skew-handling-enabled")
  var reshapeEtaThreshold: Int =
    AmberUtils.amberConfig.getInt("constants.reshape-eta-threshold")
  var reshapeTauThreshold: Int =
    AmberUtils.amberConfig.getInt("constants.reshape-tau-threshold")
}
