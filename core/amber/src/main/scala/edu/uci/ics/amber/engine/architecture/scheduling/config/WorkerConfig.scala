package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity

import java.net.URI

case object WorkerConfig {
  def generateWorkerConfigs(physicalOp: PhysicalOp): (PhysicalOpIdentity, List[WorkerConfig]) = {
    val workerCount =
      if (physicalOp.suggestedWorkerNum.isDefined) {
        physicalOp.suggestedWorkerNum.get
      } else if (physicalOp.parallelizable) {
        AmberConfig.numWorkerPerOperatorByDefault
      } else {
        1
      }

    physicalOp.id -> (0 until workerCount).toList
      .map(_ => WorkerConfig(restoreConfOpt = None, replayLogConfOpt = None))

  }
}
case class WorkerConfig(
    restoreConfOpt: Option[WorkerStateRestoreConfig] = None,
    replayLogConfOpt: Option[WorkerReplayLoggingConfig] = None
)

final case class WorkerStateRestoreConfig(readFrom: URI, replayTo: Long)

final case class WorkerReplayLoggingConfig(writeTo: URI)
