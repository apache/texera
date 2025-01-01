package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.architecture.scheduling.DefaultCostEstimator.DEFAULT_OPERATOR_COST
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.Tables.{
  WORKFLOW_EXECUTIONS,
  WORKFLOW_RUNTIME_STATISTICS,
  WORKFLOW_VERSION
}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowRuntimeStatistics
import org.jooq.types.UInteger

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.{Failure, Success, Try}

/**
  * A cost estimator should estimate a cost of running a region under the given resource constraints as units.
  */
trait CostEstimator {
  def estimate(region: Region, resourceUnits: Int): Double
}

object DefaultCostEstimator {
  val DEFAULT_OPERATOR_COST: Double = 1.0
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
  private val operatorEstimatedTimeOption = Try(
    this.getOperatorExecutionTimeInSeconds(
      this.workflowContext.workflowId.id
    )
  ) match {
    case Failure(_)      => None
    case Success(result) => result
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
          operatorEstimatedTime.getOrElse(op.id.logicalOpId.id, DEFAULT_OPERATOR_COST)
        })
        val longestRunningOpExecutionTime = opExecutionTimes.max
        longestRunningOpExecutionTime
      case None =>
        // Without past statistics (e.g., first execution), we use number of materialized ports as the cost.
        // This is independent of the schedule / resource allocator.
        region.materializedPortIds.size
    }
  }

  /**
    * Retrieve the latest successful execution to get statistics to calculate costs in DefaultCostEstimator.
    * Using the total control processing time plus data processing time of an operator as its cost.
    * If no past statistics are available (e.g., first execution), return None.
    */
  private def getOperatorExecutionTimeInSeconds(
      wid: Long
  ): Option[Map[String, Double]] = {

    val operatorEstimatedTimeOption = withTransaction(
      SqlServer
        .getInstance(
          StorageConfig.jdbcUrl,
          StorageConfig.jdbcUsername,
          StorageConfig.jdbcPassword
        )
        .createDSLContext()
    ) { context =>
      val widAsUInteger = UInteger.valueOf(wid)
      val rawStats = context
        .select(
          WORKFLOW_RUNTIME_STATISTICS.OPERATOR_ID,
          WORKFLOW_RUNTIME_STATISTICS.TIME,
          WORKFLOW_RUNTIME_STATISTICS.DATA_PROCESSING_TIME,
          WORKFLOW_RUNTIME_STATISTICS.CONTROL_PROCESSING_TIME,
          WORKFLOW_RUNTIME_STATISTICS.EXECUTION_ID
        )
        .from(WORKFLOW_RUNTIME_STATISTICS)
        .where(
          WORKFLOW_RUNTIME_STATISTICS.WORKFLOW_ID
            .eq(widAsUInteger)
            .and(
              WORKFLOW_RUNTIME_STATISTICS.EXECUTION_ID.eq(
                context
                  .select(
                    WORKFLOW_EXECUTIONS.EID
                  )
                  .from(WORKFLOW_EXECUTIONS)
                  .join(WORKFLOW_VERSION)
                  .on(WORKFLOW_VERSION.VID.eq(WORKFLOW_EXECUTIONS.VID))
                  .where(
                    WORKFLOW_VERSION.WID
                      .eq(widAsUInteger)
                      .and(WORKFLOW_EXECUTIONS.STATUS.eq(3.toByte))
                  )
                  .orderBy(WORKFLOW_EXECUTIONS.STARTING_TIME.desc())
                  .limit(1)
              )
            )
        )
        .orderBy(WORKFLOW_RUNTIME_STATISTICS.TIME, WORKFLOW_RUNTIME_STATISTICS.OPERATOR_ID)
        .fetchInto(classOf[WorkflowRuntimeStatistics])
        .asScala
        .toList
      if (rawStats.isEmpty) {
        None
      } else {
        val cumulatedStats = rawStats.foldLeft(Map.empty[String, Double]) { (acc, stat) =>
          val opTotalExecutionTime = acc.getOrElse(stat.getOperatorId, 0.0)
          acc + (stat.getOperatorId -> (opTotalExecutionTime + (stat.getDataProcessingTime
            .doubleValue() + stat.getControlProcessingTime.doubleValue()) / 1e9))
        }
        Some(cumulatedStats)
      }
    }
    operatorEstimatedTimeOption
  }
}
