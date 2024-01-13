package edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies

import edu.uci.ics.amber.engine.architecture.scheduling.{Region, RegionConfig, WorkerConfig}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

trait ResourceAllocator {
  def allocate(region: Region): (Region, Double)
}
class DefaultResourceAllocator(
    physicalPlan: PhysicalPlan,
    executionClusterInfo: ExecutionClusterInfo
) extends ResourceAllocator {

  /**
    * Allocates resources for a given region and its operators.
    *
    * This method calculates and assigns worker configurations for each operator
    * in the region. For the operators that are parallelizable, it respects the
    * suggested worker number if provided. Otherwise, it falls back to a default
    * value. Non-parallelizable operators are assigned a single worker.
    *
    * @param region The region for which to allocate resources.
    * @return A tuple containing:
    *         1) A new Region instance with new configuration.
    *         2) An estimated cost of the workflow with the new configuration,
    *         represented as a Double value (currently set to 0, but will be
    *         updated in the future).
    */
  def allocate(
      region: Region
  ): (Region, Double) = {
    val config = RegionConfig(
      region.getEffectiveOperators
        .map(physicalOpId => physicalPlan.getOperator(physicalOpId))
        .map { physicalOp =>
          {
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
            physicalOp.id -> (0 until workerCount).map(_ => WorkerConfig()).toList
          }
        }
        .toMap
    )
    (region.copy(config = Some(config)), 0)
  }
}
