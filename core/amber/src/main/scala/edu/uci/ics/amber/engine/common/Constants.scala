package edu.uci.ics.amber.engine.common

import scala.concurrent.duration._

object Constants {
  val defaultBatchSize: Int = AmberUtils.config.getInt("akka.constants.default-batch-size")
  // time interval for logging queue sizes - 30s
  val loggingQueueSizeInterval: Int =
    AmberUtils.config.getInt("akka.constants.logging-queue-size-interval")



  // Non constants: TODO: move out from Constants
  var numWorkerPerNode: Int = AmberUtils.config.getInt("akka.constants.num-worker-per-node")
  var dataVolumePerNode: Int = AmberUtils.config.getInt("akka.constants.data-volume-per-node")
  var currentWorkerNum = 0
  var currentDataSetNum = 0
  var masterNodeAddr: Option[String] = None
  var defaultTau: FiniteDuration = 10.milliseconds
}
