package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource

/**
  * A cost estimator should estimate a cost of running a region under the given resource constraints as units.
  */
trait CostEstimator {
  def estimate(region: Region, resourceUnits: Int): Double
}

/**
  * A default cost estimator using past statistics. If past statistics of a workflow are available, the cost of a region
  * is the execution time of its longest-running operator. Otherwise the cost is the number of materialized ports in the
  * region.
  */
class DefaultCostEstimator(
    workflowContext: WorkflowContext,
    val actorId: ActorVirtualIdentity
) extends CostEstimator
    with AmberLogging {

  // Requires mysql database to retrieve execution statistics, otherwise use number of materialized ports as a default.
  private val operatorEstimatedTimeOption = {
    if (StorageConfig.jdbcUsername.isEmpty) {
      None
    } else {
      WorkflowExecutionsResource.getOperatorExecutionTimeInSeconds(
        this.workflowContext.workflowId.id
      )
    }
  }

  operatorEstimatedTimeOption match {
    case None =>
      logger.info(
        s"WID: ${workflowContext.workflowId.id}, EID: ${workflowContext.executionId.id}, " +
          s"no past execution statistics available. Using number of materialized output ports as the cost. "
      )
    case Some(_) =>
  }

  override def estimate(region: Region, resourceUnits: Int): Double = {
    this.operatorEstimatedTimeOption match {
      case Some(operatorEstimatedTime) =>
        // Use past statistics (wall-clock runtime). We use the execution time of the longest-running
        // operator in each region to represent the region's execution time, and use the sum of all the regions'
        // execution time as the wall-clock runtime of the workflow.
        // This assumes a schedule is a total-order of the regions.
        val opExecutionTimes = region.getOperators.map(op => {
          operatorEstimatedTime.getOrElse(op.id.logicalOpId.id, 1.0)
        })
        val longestRunningOpExecutionTime = opExecutionTimes.max
        longestRunningOpExecutionTime
      case None =>
        // Without past statistics (e.g., first execution), we use number of materialized ports as the cost.
        // This is independent of the schedule / resource allocator.
        region.materializedPortIds.size
    }
  }
}
