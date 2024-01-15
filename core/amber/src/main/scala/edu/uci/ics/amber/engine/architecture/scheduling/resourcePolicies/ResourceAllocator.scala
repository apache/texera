package edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies

import edu.uci.ics.amber.engine.architecture.scheduling.config.{
  ChannelConfig,
  RegionConfig,
  WorkerConfig
}
import edu.uci.ics.amber.engine.architecture.scheduling.config.WorkerConfig.generateWorkerConfigs
import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.architecture.scheduling.config.ChannelConfig.toChannelConfigs
import edu.uci.ics.amber.engine.common.virtualidentity.{PhysicalLinkIdentity, PhysicalOpIdentity}
import edu.uci.ics.texera.workflow.common.workflow.{PartitionInfo, PhysicalPlan, UnknownPartition}

import scala.collection.mutable

trait ResourceAllocator {
  def allocate(region: Region): (Region, Double)
}
class DefaultResourceAllocator(
    physicalPlan: PhysicalPlan,
    executionClusterInfo: ExecutionClusterInfo
) extends ResourceAllocator {

  // a map of an operator to its output partition info
  val outputPartitionInfos = new mutable.HashMap[PhysicalOpIdentity, PartitionInfo]()

  val workerConfigs = new mutable.HashMap[PhysicalOpIdentity, List[WorkerConfig]]()
  val channelConfigs = new mutable.HashMap[PhysicalLinkIdentity, List[ChannelConfig]]()

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

    val opToWorkerConfigsMapping = region.getEffectiveOperators
      .map(physicalOpId => physicalPlan.getOperator(physicalOpId))
      .map(physicalOp => physicalOp.id -> generateWorkerConfigs(physicalOp))
      .toMap

    workerConfigs ++= opToWorkerConfigsMapping

    // assign workers to physical plan
    opToWorkerConfigsMapping.toList.foreach {
      case (physicalOpId, workerConfigs) =>
        physicalPlan.getOperator(physicalOpId).assignWorkers(workerConfigs.length)
    }
    propagatePartitionRequirementInRegion(region)

    val config = RegionConfig(
      opToWorkerConfigsMapping,
      region.getEffectiveLinks.map { physicalLinkId =>
        physicalLinkId -> toChannelConfigs(
          physicalPlan.getOperator(physicalLinkId.from).getWorkerIds,
          physicalPlan.getOperator(physicalLinkId.to).getWorkerIds,
          outputPartitionInfos(physicalLinkId.from)
        )
      }.toMap
    )

    (region.copy(config = Some(config)), 0)
  }
  private def propagatePartitionRequirementInRegion(region: Region): Unit = {
    physicalPlan
      .topologicalIterator()
      .foreach(physicalOpId => {
        val physicalOp = physicalPlan.getOperator(physicalOpId)
        println("propagate " + physicalOp.id)
        val outputPartitionInfo = if (physicalPlan.getSourceOperatorIds.contains(physicalOpId)) {
          // get output partition info of the source operator
          physicalOp.partitionRequirement.headOption.flatten.getOrElse(UnknownPartition())
        } else {
          // for each input port, enforce partition requirement
          val inputPartitionings = physicalOp.inputPorts.indices
            .flatMap(port => physicalOp.getLinksOnInputPort(port))
            .map(linkId => {
              val inputPartitionInfo = outputPartitionInfos(linkId.from)
              val outputPartitionInfo =
                physicalPlan.getOutputPartitionInfo(linkId, inputPartitionInfo)
              (linkId.toPort, outputPartitionInfo)
            })
            .groupBy({
              case (port, _) => port
            })
            .map({
              case (_, partitionInfos) => partitionInfos.map(_._2).reduce((p1, p2) => p1.merge(p2))
            })
            .toList
          assert(inputPartitionings.length == physicalOp.inputPorts.size)
          // derive the output partition info of this operator
          physicalOp.derivePartition(inputPartitionings)
        }
        outputPartitionInfos.put(physicalOpId, outputPartitionInfo)
      })
  }

}
