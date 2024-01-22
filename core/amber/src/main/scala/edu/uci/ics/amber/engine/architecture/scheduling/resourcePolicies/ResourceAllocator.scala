package edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies

import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.architecture.scheduling.config.ChannelConfig.generateChannelConfigs
import edu.uci.ics.amber.engine.architecture.scheduling.config.LinkConfig.toPartitioning
import edu.uci.ics.amber.engine.architecture.scheduling.config.WorkerConfig.generateWorkerConfigs
import edu.uci.ics.amber.engine.architecture.scheduling.config.{
  LinkConfig,
  OperatorConfig,
  RegionConfig
}
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
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
  private val outputPartitionInfos = new mutable.HashMap[PhysicalOpIdentity, PartitionInfo]()

  private val operatorConfigs = new mutable.HashMap[PhysicalOpIdentity, OperatorConfig]()
  private val linkConfigs = new mutable.HashMap[PhysicalLink, LinkConfig]()

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

    val opToOperatorConfigMapping = region.getEffectiveOperators
      .map(physicalOpId => physicalPlan.getOperator(physicalOpId))
      .map(physicalOp => physicalOp.id -> OperatorConfig(generateWorkerConfigs(physicalOp)))
      .toMap

    operatorConfigs ++= opToOperatorConfigMapping

    propagatePartitionRequirement(region)

    val linkToLinkConfigMapping = region.getEffectiveLinks.map { physicalLink =>
      physicalLink -> LinkConfig(
        generateChannelConfigs(
          operatorConfigs(physicalLink.fromOpId).workerConfigs.map(_.workerId),
          operatorConfigs(physicalLink.toOpId).workerConfigs.map(_.workerId),
          outputPartitionInfos(physicalLink.fromOpId)
        ),
        toPartitioning(
          operatorConfigs(physicalLink.toOpId).workerConfigs.map(_.workerId),
          outputPartitionInfos(physicalLink.fromOpId)
        )
      )
    }.toMap

    linkConfigs ++= linkToLinkConfigMapping

    val config = RegionConfig(opToOperatorConfigMapping, linkToLinkConfigMapping)

    (region.copy(config = Some(config)), 0)
  }

  /**
    * This method propagates partitioning requirements in the PhysicalPlan DAG.
    *
    * This method is invoked once for each region, and only propagate partitioning requirements within
    * the region. For example, suppose we have the following physical Plan:
    *
    *     A ->
    *           HJ
    *     B ->
    * The link A->HJ will be propagated in the first region. The link B->HJ will be propagate in the second region.
    * The output partition info of HJ will be derived after both links are propagated, which is in the second region.
    *
    * This method also applies the following optimization:
    *  - if the upstream of the link has the same partitioning requirement as that of the downstream, and their
    *  number of workers are equal, then the partitioning on this link can be optimized to OneToOne.
    */
  private def propagatePartitionRequirement(region: Region): Unit = {
    physicalPlan
      .topologicalIterator()
      .filter(physicalOpId => region.getEffectiveOperators.contains(physicalOpId))
      .foreach(physicalOpId => {
        val physicalOp = physicalPlan.getOperator(physicalOpId)
        val outputPartitionInfo = if (physicalPlan.getSourceOperatorIds.contains(physicalOpId)) {
          Some(physicalOp.partitionRequirement.headOption.flatten.getOrElse(UnknownPartition()))
        } else {
          val inputPartitionInfos = physicalOp.inputPorts.keys
            .flatMap((portId: PortIdentity) => {
              physicalOp
                .getInputLinks(Some(portId))
                .filter(link => region.getEffectiveLinks.contains(link))
                .map(link => {
                  val upstreamInputPartitionInfo = outputPartitionInfos(link.fromOpId)
                  val upstreamOutputPartitionInfo = physicalPlan.getOutputPartitionInfo(
                    link,
                    upstreamInputPartitionInfo,
                    operatorConfigs.map {
                      case (opId, operatorConfig) => opId -> operatorConfig.workerConfigs.length
                    }.toMap
                  )
                  (link.toPortId, upstreamOutputPartitionInfo)
                })
            })
            // group upstream partition infos by input port of this physicalOp
            .groupBy(_._1)
            .values
            .toList
            // if there are multiple partition infos on an input port, reduce them to once
            .map(_.map(_._2).reduce((p1, p2) => p1.merge(p2)))

          if (inputPartitionInfos.length == physicalOp.inputPorts.size) {
            // derive the output partition info with all the input partition infos
            Some(physicalOp.derivePartition(inputPartitionInfos))
          } else {
            None
          }

        }
        if (outputPartitionInfo.isDefined) {
          outputPartitionInfos.put(physicalOpId, outputPartitionInfo.get)
        }
      })
  }
}
