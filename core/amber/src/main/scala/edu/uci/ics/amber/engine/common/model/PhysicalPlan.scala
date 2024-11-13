package edu.uci.ics.amber.engine.common.model

import com.fasterxml.jackson.annotation.JsonIgnore
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.model.RPGSearchPruningUtils.{getBridges, getMaxChains}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, OperatorIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PartitionInfo, UnknownPartition}
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.traverse.TopologicalOrderIterator
import org.jgrapht.util.SupplierUtil

import scala.jdk.CollectionConverters.{IteratorHasAsScala, SetHasAsScala}

object PhysicalPlan {
  def apply(context: WorkflowContext, logicalPlan: LogicalPlan): PhysicalPlan = {

    var physicalPlan = PhysicalPlan(operators = Set.empty, links = Set.empty)

    logicalPlan.getTopologicalOpIds.asScala.foreach(logicalOpId => {
      val logicalOp = logicalPlan.getOperator(logicalOpId)
      logicalOp.setContext(context)

      val subPlan = logicalOp.getPhysicalPlan(context.workflowId, context.executionId)
      subPlan
        .topologicalIterator()
        .map(subPlan.getOperator)
        .foreach({ physicalOp =>
          {
            val externalLinks = logicalPlan
              .getUpstreamLinks(logicalOp.operatorIdentifier)
              .filter(link => physicalOp.inputPorts.contains(link.toPortId))
              .flatMap { link =>
                physicalPlan
                  .getPhysicalOpsOfLogicalOp(link.fromOpId)
                  .find(_.outputPorts.contains(link.fromPortId))
                  .map(fromOp =>
                    PhysicalLink(fromOp.id, link.fromPortId, physicalOp.id, link.toPortId)
                  )
              }

            val internalLinks = subPlan.getUpstreamPhysicalLinks(physicalOp.id)

            // Add the operator to the physical plan
            physicalPlan = physicalPlan.addOperator(physicalOp.propagateSchema())

            // Add all the links to the physical plan
            physicalPlan = (externalLinks ++ internalLinks)
              .foldLeft(physicalPlan) { (plan, link) => plan.addLink(link) }
          }
        })
    })
    physicalPlan
  }

}

case class PhysicalPlan(
    operators: Set[PhysicalOp],
    links: Set[PhysicalLink]
) extends LazyLogging {

  @transient private lazy val operatorMap: Map[PhysicalOpIdentity, PhysicalOp] =
    operators.map(o => (o.id, o)).toMap

  // the dag will be re-computed again once it reaches the coordinator.
  @transient lazy val dag: DirectedAcyclicGraph[PhysicalOpIdentity, PhysicalLink] = {
    val jgraphtDag = new DirectedAcyclicGraph[PhysicalOpIdentity, PhysicalLink](
      null, // vertexSupplier
      SupplierUtil.createSupplier(classOf[PhysicalLink]), // edgeSupplier
      false, // weighted
      true // allowMultipleEdges
    )
    operatorMap.foreach(op => jgraphtDag.addVertex(op._1))
    links.foreach(l => jgraphtDag.addEdge(l.fromOpId, l.toOpId, l))
    jgraphtDag
  }

  // These lazy vals are used by CostBasedRegionPlanGenerator for search pruning.
  @transient lazy val maxChains: Set[Set[PhysicalLink]] = getMaxChains(this.dag)

  @transient lazy val nonMaterializedBlockingAndDependeeLinks: Set[PhysicalLink] =
    this.getNonMaterializedBlockingAndDependeeLinks

  @transient lazy val nonBlockingLinks: Set[PhysicalLink] = this.getNonBlockingLinks

  @transient lazy val nonBridgeNonBlockingLinks: Set[PhysicalLink] =
    this.getNonBridgeNonBlockingLinks

  def getSourceOperatorIds: Set[PhysicalOpIdentity] =
    operatorMap.keys.filter(op => dag.inDegreeOf(op) == 0).toSet

  def getPhysicalOpsOfLogicalOp(logicalOpId: OperatorIdentity): List[PhysicalOp] = {
    topologicalIterator()
      .filter(physicalOpId => physicalOpId.logicalOpId == logicalOpId)
      .map(physicalOpId => getOperator(physicalOpId))
      .toList
  }

  def getOperator(physicalOpId: PhysicalOpIdentity): PhysicalOp = operatorMap(physicalOpId)

  /**
    * returns a sub-plan that contains the specified operators and the links connected within these operators
    */
  def getSubPlan(subOperators: Set[PhysicalOpIdentity]): PhysicalPlan = {
    val newOps = operators.filter(op => subOperators.contains(op.id))
    val newLinks =
      links.filter(link =>
        subOperators.contains(link.fromOpId) && subOperators.contains(link.toOpId)
      )
    PhysicalPlan(newOps, newLinks)
  }

  def getUpstreamPhysicalOpIds(physicalOpId: PhysicalOpIdentity): Set[PhysicalOpIdentity] = {
    dag.incomingEdgesOf(physicalOpId).asScala.map(e => dag.getEdgeSource(e)).toSet
  }

  def getUpstreamPhysicalLinks(physicalOpId: PhysicalOpIdentity): Set[PhysicalLink] = {
    links.filter(l => l.toOpId == physicalOpId)
  }

  def getDownstreamPhysicalLinks(physicalOpId: PhysicalOpIdentity): Set[PhysicalLink] = {
    links.filter(l => l.fromOpId == physicalOpId)
  }

  def topologicalIterator(): Iterator[PhysicalOpIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }
  def addOperator(physicalOp: PhysicalOp): PhysicalPlan = {
    this.copy(operators = Set(physicalOp) ++ operators)
  }

  def addLink(link: PhysicalLink): PhysicalPlan = {
    val formOp = operatorMap(link.fromOpId)
    val (_, _, outputSchema) = formOp.outputPorts(link.fromPortId)
    val newFromOp = formOp.addOutputLink(link)
    val newToOp = getOperator(link.toOpId)
      .addInputLink(link)
      .propagateSchema(outputSchema.toOption.map(schema => (link.toPortId, schema)))

    val newOperators = operatorMap +
      (link.fromOpId -> newFromOp) +
      (link.toOpId -> newToOp)
    this.copy(newOperators.values.toSet, links ++ Set(link))
  }

  def removeLink(
      link: PhysicalLink
  ): PhysicalPlan = {
    val fromOpId = link.fromOpId
    val toOpId = link.toOpId
    val newOperators = operatorMap +
      (fromOpId -> getOperator(fromOpId).removeOutputLink(link)) +
      (toOpId -> getOperator(toOpId).removeInputLink(link))
    this.copy(operators = newOperators.values.toSet, links.filter(l => l != link))
  }

  def setOperator(physicalOp: PhysicalOp): PhysicalPlan = {
    this.copy(operators = (operatorMap + (physicalOp.id -> physicalOp)).values.toSet)
  }

  def getPhysicalOpByWorkerId(workerId: ActorVirtualIdentity): PhysicalOp =
    getOperator(VirtualIdentityUtils.getPhysicalOpId(workerId))

  def getLinksBetween(
      from: PhysicalOpIdentity,
      to: PhysicalOpIdentity
  ): Set[PhysicalLink] = {
    links.filter(link => link.fromOpId == from && link.toOpId == to)

  }

  def getOutputPartitionInfo(
      link: PhysicalLink,
      upstreamPartitionInfo: PartitionInfo,
      opToWorkerNumberMapping: Map[PhysicalOpIdentity, Int]
  ): PartitionInfo = {
    val fromPhysicalOp = getOperator(link.fromOpId)
    val toPhysicalOp = getOperator(link.toOpId)

    // make sure this input is connected to this port
    assert(
      toPhysicalOp
        .getInputLinks(Some(link.toPortId))
        .map(link => link.fromOpId)
        .contains(fromPhysicalOp.id)
    )

    // partition requirement of this PhysicalOp on this input port
    val requiredPartitionInfo =
      toPhysicalOp.partitionRequirement
        .lift(link.toPortId.id)
        .flatten
        .getOrElse(UnknownPartition())

    // the upstream partition info satisfies the requirement, and number of worker match
    if (
      upstreamPartitionInfo.satisfies(requiredPartitionInfo) && opToWorkerNumberMapping.getOrElse(
        fromPhysicalOp.id,
        0
      ) == opToWorkerNumberMapping.getOrElse(toPhysicalOp.id, 0)
    ) {
      upstreamPartitionInfo
    } else {
      // we must re-distribute the input partitions
      requiredPartitionInfo

    }
  }

  private def isMaterializedLink(link: PhysicalLink): Boolean = {
    getOperator(link.toOpId).isSinkOperator
  }

  /**
    * The effective blocking edges also include dependee links and do not include links created as a result of adding
    * materialization operators.
    */
  private def getNonMaterializedBlockingAndDependeeLinks: Set[PhysicalLink] = {
    operators
      .flatMap { physicalOp =>
        {
          getUpstreamPhysicalOpIds(physicalOp.id)
            .flatMap { upstreamPhysicalOpId =>
              links
                .filter(link =>
                  link.fromOpId == upstreamPhysicalOpId && link.toOpId == physicalOp.id
                )
                .filter(link =>
                  !isMaterializedLink(link) && (getOperator(physicalOp.id).isInputLinkDependee(
                    link
                  ) || getOperator(upstreamPhysicalOpId).isOutputLinkBlocking(link))
                )
            }
        }
      }
  }

  def getDependeeLinks: Set[PhysicalLink] = {
    operators
      .flatMap { physicalOp =>
        {
          getUpstreamPhysicalOpIds(physicalOp.id)
            .flatMap { upstreamPhysicalOpId =>
              links
                .filter(link =>
                  link.fromOpId == upstreamPhysicalOpId && link.toOpId == physicalOp.id
                )
                .filter(link => getOperator(physicalOp.id).isInputLinkDependee(link))
            }
        }
      }
  }

  /**
    * create a DAG similar to the physical DAG but with all dependee links removed.
    */
  @JsonIgnore // this is needed to prevent the serialization issue
  def getDependeeLinksRemovedDAG: PhysicalPlan = {
    this.copy(operators, links.diff(getDependeeLinks))
  }

  private def getNonBlockingLinks: Set[PhysicalLink] = {
    this.links.diff(getNonMaterializedBlockingAndDependeeLinks)
  }

  /**
    * A non-blocking link is a clean edge if it is not in the same undirected cycle as in a blocking link, where an
    * undirected cycle of a directed graph means a subgraph that forms a simple cycle after ignoring the directionality
    * of its edges. A bridge is a special case of a clean edge. Due to the implementation complexity of finding
    * undirected cycles, we use only bridges here.
    *
    * @return All non-blocking links that are not bridges.
    */
  private def getNonBridgeNonBlockingLinks: Set[PhysicalLink] = {
    this.getNonBlockingLinks.diff(getBridges(this.dag, this.getNonBlockingLinks))
  }

}
