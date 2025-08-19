package edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies

import edu.uci.ics.amber.config.ApplicationConfig
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.architecture.scheduling.config._
import edu.uci.ics.amber.util.VirtualIdentityUtils
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.Tables.{WORKFLOW_EXECUTIONS, WORKFLOW_VERSION}

import java.net.URI
import scala.collection.mutable

class GreedyResourceAllocator(
                                physicalPlan: PhysicalPlan,
                                executionClusterInfo: ExecutionClusterInfo,
                                workflowSettings: WorkflowSettings,
                                workflowContext: WorkflowContext
                              ) extends ResourceAllocator {

  // a map of a physical link to the partition info of the upstream/downstream of this link
  private val linkPartitionInfos = new mutable.HashMap[PhysicalLink, PartitionInfo]()

  private val operatorConfigs = new mutable.HashMap[PhysicalOpIdentity, OperatorConfig]()
  private val linkConfigs = new mutable.HashMap[PhysicalLink, LinkConfig]()

  /**
   * Allocates resources for a given region and its operators.
   *
   * This method calculates and assigns worker configurations for each operator
   * in the region.
   * For the operators that are parallelizable, it respects the
   * suggested worker number if provided.
   * Non-parallelizable operators are assigned a single worker.
   * For parallelizable operators without suggestions, the slowest operator
   * is assigned with one more worker.
   * Automatic adjustment will not result the total number of workers exceeds
   * configured core to worker ratio * configured cpu cores.
   * @param region The region for which to allocate resources.
   * @return A tuple containing:
   *         1) A new Region instance with new resource configuration.
   *         2) An estimated cost of the workflow with the new resource configuration,
   *         represented as a Double value (currently set to 0, but will be
   *         updated in the future).
   */
  def allocate(
                region: Region
              ): (Region, Double) = {
    val statsOpt = getOperatorExecutionStats(this.workflowContext.workflowId.id)
    val maxWorkerUpperBound = executionClusterInfo.availableNumberOfCores * executionClusterInfo.coreToWorkerRatio
    val operatorList = region.getOperators

    val opToOperatorConfigMapping: Map[PhysicalOpIdentity, OperatorConfig] = statsOpt match {

      case Some(stats) =>
        val opToWorkerList = mutable.HashMap[PhysicalOpIdentity, List[WorkerConfig]]()

        val greedyCandidates = operatorList.filter(op =>
          op.parallelizable && op.suggestedWorkerNum.isEmpty && stats.contains(op.id.logicalOpId.id)
        )

        val slowestOpIdStrOpt =
          greedyCandidates
            .flatMap(op => stats.get(op.id.logicalOpId.id).map { case (rt, _) => op.id.logicalOpId.id -> rt })
            .maxByOption(_._2)
            .map(_._1)

        val basicWorkerCounts: Map[PhysicalOpIdentity, Int] = operatorList.map { op =>
          val opIdStr = op.id.logicalOpId.id
          val baseCount =
            if (!op.parallelizable) 1
            else if (op.suggestedWorkerNum.isDefined) op.suggestedWorkerNum.get
            else if (stats.contains(opIdStr)) stats(opIdStr)._2
            else ApplicationConfig.numWorkerPerOperatorByDefault
          op.id -> baseCount
        }.toMap

        val currentTotal = basicWorkerCounts.values.sum

        val addingAllowed = slowestOpIdStrOpt.isDefined && currentTotal + 1 <= maxWorkerUpperBound

        operatorList.foreach { op =>
          val opIdStr = op.id.logicalOpId.id
          val basicWorkerCount = basicWorkerCounts(op.id)
          val updatedWorkerCount =
            if (addingAllowed && slowestOpIdStrOpt.contains(opIdStr)) basicWorkerCount + 1 else basicWorkerCount
          val workers = (0 until updatedWorkerCount).map { idx =>
            WorkerConfig(
              VirtualIdentityUtils.createWorkerIdentity(op.workflowId, op.id, idx)
            )
          }.toList
          opToWorkerList(op.id) = workers
        }

        opToWorkerList.map {
          case (opId, workerList) => opId -> OperatorConfig(workerList)
        }.toMap

      case None =>
        region.getOperators
          .map(op => op.id -> OperatorConfig(WorkerConfig.generateDefaultWorkerConfigs(op)))
          .toMap
    }

    operatorConfigs ++= opToOperatorConfigMapping

    val updatedLinkPartitionInfos = propagatePartitionRequirement(region, physicalPlan, operatorConfigs.toMap, linkPartitionInfos.toMap)

    linkPartitionInfos ++= updatedLinkPartitionInfos

    val linkToLinkConfigMapping =
      getLinkConfigs(region, operatorConfigs.toMap, linkPartitionInfos.toMap, workflowSettings)

    linkConfigs ++= linkToLinkConfigMapping

    val portConfigs: Map[GlobalPortIdentity, PortConfig] = getPortConfigs(region, operatorConfigs.toMap, workflowSettings)

    val resourceConfig = ResourceConfig(
      opToOperatorConfigMapping,
      linkToLinkConfigMapping,
      portConfigs
    )

    (region.copy(resourceConfig = Some(resourceConfig)), 0)
  }

  private def getOperatorExecutionStats(wid: Long): Option[Map[String, (Double, Int)]] = {
    val uriOpt: Option[String] =
      withTransaction(SqlServer.getInstance().createDSLContext()) { context =>
        val row = context
          .select(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI)
          .from(WORKFLOW_EXECUTIONS)
          .join(WORKFLOW_VERSION)
          .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
          .where(
            WORKFLOW_VERSION.WID.eq(wid.toInt)
              .and(WORKFLOW_EXECUTIONS.STATUS.eq(3.toByte)) // 成功状态
          )
          .orderBy(WORKFLOW_EXECUTIONS.STARTING_TIME.desc())
          .limit(1)
          .fetchOne()

        Option(row).map(_.get(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI))
      }

    uriOpt.flatMap { uriStr =>
      if (uriStr == null || uriStr.trim.isEmpty) {
        None
      } else {
        val stats = readStatsFromUri(uriStr)
        if (stats.isEmpty) None else Some(stats)
      }
    }
  }


  private def readStatsFromUri(uriStr: String): Map[String, (Double, Int)] = {
    val uri = new URI(uriStr)
    val document = DocumentFactory.openDocument(uri)

    document._1.get().foldLeft(Map.empty[String, (Double, Int)]) { (acc, tuple) =>
      val record = tuple.asInstanceOf[Tuple]
      val operatorId = record.getField(0).asInstanceOf[String]
      val dataProcessingTime = record.getField(6).asInstanceOf[Long]
      val controlProcessingTime = record.getField(7).asInstanceOf[Long]
      val workerNum = record.getField(9).asInstanceOf[Int]
      val execTime = (dataProcessingTime + controlProcessingTime) / 1e9 / workerNum

      acc.get(operatorId) match {
        case Some((prevTime, _)) => acc + (operatorId -> (Math.max(prevTime, execTime), workerNum))
        case None                => acc + (operatorId -> (execTime, workerNum))
      }
    }
  }
}
