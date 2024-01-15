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
          workerConfigs.getOrElse(physicalLinkId.from, List()).map(_.workerId),
          workerConfigs.getOrElse(physicalLinkId.to, List()).map(_.workerId),
          outputPartitionInfos(physicalLinkId.from)
        )
      }.toMap
    )

    (region.copy(config = Some(config)), 0)
  }

  /**
    * This method propagates partitioning requirements in the PhysicalPlan DAG.
    *
    * To be relaxed:
    *   This method is supposed to be invoked once for each region, and only propagate partitioning requirements within
    *   the region, however, we can only do full physical plan propagation due to link dependency from other regions.
    *
    *   For example, suppose we have the following physical Plan:
    *
    *     A ->
    *           HJ
    *     B ->
    *
    *   When propagate the first region A -> HJ, it requires the partitioning requirement of all HJ's input links,
    *   which contains the link B-HJ from the second region.
    *
    * This method also applies the following optimization:
    *  - if the upstream of the link has the same partitioning requirement as that of the downstream, and their
    *  number of workers are equal, then the partitioning on this link can be optimized to OneToOne.
    */
  private def propagatePartitionRequirementInRegion(region: Region): Unit = {
    physicalPlan
      .topologicalIterator()
      .foreach(physicalOpId => {
        val physicalOp = physicalPlan.getOperator(physicalOpId)
        val outputPartitionInfo = if (physicalPlan.getSourceOperatorIds.contains(physicalOpId)) {
          physicalOp.partitionRequirement.headOption.flatten.getOrElse(UnknownPartition())
        } else {
          val inputPartitionings = physicalOp.inputPorts.indices.toList
            .flatMap(port =>
              physicalOp
                .getLinksOnInputPort(port)
                .map(linkId => {
                  val upstreamInputPartitionInfo = outputPartitionInfos(linkId.from)
                  val upstreamOutputPartitionInfo = physicalPlan.getOutputPartitionInfo(
                    linkId,
                    upstreamInputPartitionInfo,
                    workerConfigs.map {
                      case (opId, workerConfigs) => opId -> workerConfigs.size
                    }.toMap
                  )
                  (linkId.toPort, upstreamOutputPartitionInfo)
                })
            )
            .groupBy(_._1)
            .values
            .toList
            .map(_.map(_._2).reduce((p1, p2) => p1.merge(p2)))

          assert(inputPartitionings.length == physicalOp.inputPorts.size)
          physicalOp.derivePartition(inputPartitionings)
        }
        outputPartitionInfos.put(physicalOpId, outputPartitionInfo)
      })
  }

}
