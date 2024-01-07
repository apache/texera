package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity

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

    physicalOp.id -> (0 until workerCount).map(_ => WorkerConfig()).toList
  }
}
case class WorkerConfig(
    logStorageType: String = AmberConfig.faultToleranceLogRootFolder,
    replayTo: Option[Long] = None
)
