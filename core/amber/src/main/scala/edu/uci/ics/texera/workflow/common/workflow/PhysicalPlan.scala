package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.jdk.CollectionConverters.{IteratorHasAsScala, SetHasAsScala}

object PhysicalPlan {

  def apply(operatorList: Array[PhysicalOp], links: Array[PhysicalLink]): PhysicalPlan = {
    new PhysicalPlan(operatorList.toSet, links.toSet)
  }

  def apply(context: WorkflowContext, logicalPlan: LogicalPlan): PhysicalPlan = {

    var physicalPlan = PhysicalPlan(operators = Set.empty, links = Set.empty)

    logicalPlan.operators.foreach(op => {
      val subPlan =
        op.getPhysicalPlan(
          context.workflowId,
          context.executionId
        )
      physicalPlan = physicalPlan.addSubPlan(subPlan)
    })

    // connect external links
    logicalPlan.links.foreach(link => {
      val fromOp = physicalPlan.getPhysicalOpForOutputPort(link.fromOpId, link.fromPortId)
      val toOp = physicalPlan.getPhysicalOpForInputPort(link.toOpId, link.toPortId)
      physicalPlan =
        physicalPlan.addLink(PhysicalLink(fromOp.id, link.fromPortId, toOp.id, link.toPortId))
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
  @transient lazy val dag: DirectedAcyclicGraph[PhysicalOpIdentity, DefaultEdge] = {
    val jgraphtDag = new DirectedAcyclicGraph[PhysicalOpIdentity, DefaultEdge](classOf[DefaultEdge])
    operatorMap.foreach(op => jgraphtDag.addVertex(op._1))
    links.foreach(l => jgraphtDag.addEdge(l.fromOpId, l.toOpId))
    jgraphtDag
  }

  def getSourceOperatorIds: Set[PhysicalOpIdentity] =
    operatorMap.keys.filter(op => dag.inDegreeOf(op) == 0).toSet

  def getSinkOperatorIds: Set[PhysicalOpIdentity] =
    operatorMap.keys
      .filter(op => dag.outDegreeOf(op) == 0)
      .toSet

  private def getPhysicalOpForInputPort(
      logicalOpId: OperatorIdentity,
      portId: PortIdentity
  ): PhysicalOp = {
    assert(!portId.internal, "only support external port")
    val candidatePhysicalOps =
      getPhysicalOpsOfLogicalOp(logicalOpId).filter(op => op.inputPorts.contains(portId))
    assert(
      candidatePhysicalOps.size == 1,
      s"find ${candidatePhysicalOps.size} input port(s) with id = $portId for operator $logicalOpId"
    )
    candidatePhysicalOps.head
  }

  private def getPhysicalOpForOutputPort(
      logicalOpId: OperatorIdentity,
      portId: PortIdentity
  ): PhysicalOp = {
    assert(!portId.internal, "only support external port")
    val candidatePhysicalOps =
      getPhysicalOpsOfLogicalOp(logicalOpId).filter(op => op.outputPorts.contains(portId))
    assert(
      candidatePhysicalOps.size == 1,
      s"find ${candidatePhysicalOps.size} output port(s) with id = $portId for operator $logicalOpId"
    )
    candidatePhysicalOps.head
  }

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

  def getDownstreamPhysicalOpIds(physicalOpId: PhysicalOpIdentity): Set[PhysicalOpIdentity] = {
    dag.outgoingEdgesOf(physicalOpId).asScala.map(e => dag.getEdgeTarget(e)).toSet
  }

  def getDownstreamPhysicalLinks(physicalOpId: PhysicalOpIdentity): Set[PhysicalLink] = {
    links.filter(l => l.fromOpId == physicalOpId)
  }

  def getDescendantPhysicalOpIds(physicalOpId: PhysicalOpIdentity): Set[PhysicalOpIdentity] = {
    dag.getDescendants(physicalOpId).asScala.toSet
  }
  def topologicalIterator(): Iterator[PhysicalOpIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }
  def addOperator(physicalOp: PhysicalOp): PhysicalPlan = {
    this.copy(operators = Set(physicalOp) ++ operators)
  }

  def addLink(link: PhysicalLink): PhysicalPlan = {
    val newOperators = operatorMap +
      (link.fromOpId -> getOperator(link.fromOpId).addOutputLink(link)) +
      (link.toOpId -> getOperator(link.toOpId).addInputLink(link))
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

  private def addSubPlan(subPlan: PhysicalPlan): PhysicalPlan = {
    var resultPlan = this.copy(operators, links)
    // add all physical operators to physical DAG
    subPlan.operators.foreach(op => resultPlan = resultPlan.addOperator(op))
    // connect intra-operator links
    subPlan.links.foreach((physicalLink: PhysicalLink) =>
      resultPlan = resultPlan.addLink(physicalLink)
    )
    resultPlan
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

  /**
    * create a DAG similar to the physical DAG but with all blocking links removed.
    */
  def removeBlockingLinks(): PhysicalPlan = {
    val linksToRemove = operators
      .flatMap { physicalOp =>
        {
          getUpstreamPhysicalOpIds(physicalOp.id)
            .flatMap { upstreamPhysicalOpId =>
              links
                .filter(link =>
                  link.fromOpId == upstreamPhysicalOpId && link.toOpId == physicalOp.id
                )
                .filter(link => getOperator(physicalOp.id).isInputLinkBlocking(link))
            }
        }
      }

    this.copy(operators, links.diff(linksToRemove))
  }

  def setOperatorUnblockPort(
      physicalOpId: PhysicalOpIdentity,
      portIdToRemove: PortIdentity
  ): PhysicalPlan = {
    val physicalOp = getOperator(physicalOpId)
    physicalOp.copy(blockingInputs =
      physicalOp.blockingInputs.filter(port => port != portIdToRemove)
    )
    this.copy(operators = operators ++ Set(physicalOp))
  }

  def areAllInputBlocking(physicalOpId: PhysicalOpIdentity): Boolean = {
    val upstreamPhysicalLinkIds = getUpstreamPhysicalLinks(physicalOpId)
    upstreamPhysicalLinkIds.nonEmpty && upstreamPhysicalLinkIds.forall { upstreamPhysicalLinkId =>
      getOperator(physicalOpId).isInputLinkBlocking(upstreamPhysicalLinkId)
    }
  }

}
