package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.util.VirtualIdentityUtils
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

case object WorkerConfig {
  def generateWorkerConfigs(physicalOp: PhysicalOp): List[WorkerConfig] = {
    val workerCount = if (physicalOp.parallelizable) {
      physicalOp.suggestedWorkerNum match {
        // Keep suggested number of workers
        case Some(num) => num
        // If no suggested number, use default value
        case None => AmberConfig.numWorkerPerOperatorByDefault
      }
    } else {
      // Non parallelizable operator has only 1 worker
      1
    }

    (0 until workerCount).toList.map(idx =>
      WorkerConfig(
        VirtualIdentityUtils.createWorkerIdentity(physicalOp.workflowId, physicalOp.id, idx)
      )
    )
  }
}

case class WorkerConfig(
    workerId: ActorVirtualIdentity
)
