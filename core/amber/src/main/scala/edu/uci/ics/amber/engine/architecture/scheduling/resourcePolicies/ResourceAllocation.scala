package edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies

import edu.uci.ics.amber.engine.architecture.scheduling.{Region, RegionConfig, WorkerConfig}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

class ResourceAllocator(physicalPlan: PhysicalPlan, executionClusterInfo: ExecutionClusterInfo) {

  def allocate(
      region: Region
  ): (Region, Double) = {
    val config = RegionConfig(
      region.getEffectiveOperators
        .map(physicalOpId => physicalPlan.getOperator(physicalOpId))
        .map { physicalOp =>
          {
            val workerCount =
              if (physicalOp.parallelizable) {
                if (physicalOp.suggestedWorkerNum.isDefined) {
                  physicalOp.suggestedWorkerNum.get
                } else {
                  AmberConfig.numWorkerPerOperatorByDefault
                }
              } else {
                1
              }
            physicalOp.id -> (0 until workerCount).map(_ => WorkerConfig()).toList
          }
        }
        .toMap
    )
    (region.copy(config = Some(config)), 0)
  }
}
