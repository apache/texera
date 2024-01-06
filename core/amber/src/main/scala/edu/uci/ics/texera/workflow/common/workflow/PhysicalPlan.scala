package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.common.AmberConfig.defaultBatchSize
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalLinkIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.JavaConverters._
import scala.collection.mutable

object PhysicalPlan {

  def apply(operatorList: Array[PhysicalOp], links: Array[PhysicalLink]): PhysicalPlan = {
    new PhysicalPlan(operatorList.toSet, links.toSet)
  }

  def apply(context: WorkflowContext, logicalPlan: LogicalPlan): PhysicalPlan = {

    var physicalPlan = PhysicalPlan(operators = Set.empty, links = Set.empty)

    logicalPlan.operators.foreach(op => {
      val subPlan =
        op.getPhysicalPlan(
          context.executionId,
          logicalPlan.getOpSchemaInfo(op.operatorIdentifier)
        )
      physicalPlan = physicalPlan.addSubPlan(subPlan)
    })

    // connect inter-operator links
    logicalPlan.links.foreach(link => {
      val fromLogicalOp = link.origin.operatorId
      val fromPort = link.origin.portOrdinal
      val fromPortName = logicalPlan.operators
        .filter(op => op.operatorIdentifier == link.origin.operatorId)
        .head
        .operatorInfo
        .outputPorts(fromPort)
        .displayName
      val fromOp = physicalPlan.getPhysicalOpForOutputPort(fromLogicalOp, fromPortName)

      val toLogicalOp = logicalPlan.getOperator(link.destination.operatorId).operatorIdentifier
      val toPort = link.destination.portOrdinal
      val toPortName = logicalPlan.operators
        .filter(op => op.operatorIdentifier == link.destination.operatorId)
        .head
        .operatorInfo
        .inputPorts(toPort)
        .displayName
      val toOp = physicalPlan.getPhysicalOpForInputPort(toLogicalOp, toPortName)

      physicalPlan = physicalPlan.addLink(fromOp, fromPort, toOp, toPort)
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

  @transient private lazy val linkMap: Map[PhysicalLinkIdentity, PhysicalLink] =
    links.map(link => link.id -> link).toMap

  // the dag will be re-computed again once it reaches the coordinator.
  @transient lazy val dag: DirectedAcyclicGraph[PhysicalOpIdentity, DefaultEdge] = {
    val jgraphtDag = new DirectedAcyclicGraph[PhysicalOpIdentity, DefaultEdge](classOf[DefaultEdge])
    operatorMap.foreach(op => jgraphtDag.addVertex(op._1))
    links.foreach(l => jgraphtDag.addEdge(l.fromOp.id, l.toOp.id))
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
      portName: String
  ): PhysicalOp = {
    val candidatePhysicalOps = getPhysicalOpsOfLogicalOp(logicalOpId).filter(op =>
      op.inputPorts.map(_.displayName).contains(portName)
    )
    assert(
      candidatePhysicalOps.size == 1,
      s"find no or multiple input port with name = $portName for operator $logicalOpId"
    )
    candidatePhysicalOps.head
  }

  private def getPhysicalOpForOutputPort(
      logicalOpId: OperatorIdentity,
      portName: String
  ): PhysicalOp = {
    val candidatePhysicalOps = getPhysicalOpsOfLogicalOp(logicalOpId).filter(op =>
      op.outputPorts.map(_.displayName).contains(portName)
    )
    assert(
      candidatePhysicalOps.size == 1,
      s"find no or multiple output port with name = $portName for operator $logicalOpId"
    )
    candidatePhysicalOps.head
  }

  def getPhysicalOpsOfLogicalOp(logicalOpId: OperatorIdentity): List[PhysicalOp] = {
    topologicalIterator()
      .filter(physicalOpId => physicalOpId.logicalOpId == logicalOpId)
      .map(physicalOpId => getOperator(physicalOpId))
      .toList
  }

  def getSinglePhysicalOpOfLogicalOp(logicalOpId: OperatorIdentity): PhysicalOp = {
    val physicalOps = getPhysicalOpsOfLogicalOp(logicalOpId)
    if (physicalOps.size != 1) {
      val msg =
        s"logical operator $logicalOpId has ${physicalOps.size} physical operators, expecting a single one"
      throw new RuntimeException(msg)
    }
    physicalOps.head
  }

  def getOperator(physicalOpId: PhysicalOpIdentity): PhysicalOp = operatorMap(physicalOpId)

  def getLink(linkId: PhysicalLinkIdentity): PhysicalLink = linkMap(linkId)

  /**
    * returns a sub-plan that contains the specified operators and the links connected within these operators
    */
  def getSubPlan(subOperators: Set[PhysicalOpIdentity]): PhysicalPlan = {
    val newOps = operators.filter(op => subOperators.contains(op.id))
    val newLinks =
      links.filter(link =>
        subOperators.contains(link.fromOp.id) && subOperators.contains(link.toOp.id)
      )
    PhysicalPlan(newOps, newLinks)
  }

  def getUpstreamPhysicalOpIds(physicalOpId: PhysicalOpIdentity): Set[PhysicalOpIdentity] = {
    dag.incomingEdgesOf(physicalOpId).asScala.map(e => dag.getEdgeSource(e)).toSet
  }

  def getUpstreamPhysicalLinkIds(physicalOpId: PhysicalOpIdentity): Set[PhysicalLinkIdentity] = {
    links.filter(l => l.toOp.id == physicalOpId).map(_.id)
  }

  def getDownstreamPhysicalOpIds(physicalOpId: PhysicalOpIdentity): Set[PhysicalOpIdentity] = {
    dag.outgoingEdgesOf(physicalOpId).asScala.map(e => dag.getEdgeTarget(e)).toSet
  }

  def getDownstreamPhysicalLinkIds(physicalOpId: PhysicalOpIdentity): Set[PhysicalLinkIdentity] = {
    links.filter(l => l.fromOp.id == physicalOpId).map(_.id)
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

  def addLink(physicalLink: PhysicalLink): PhysicalPlan = {
    addLink(physicalLink.fromOp, physicalLink.fromPort, physicalLink.toOp, physicalLink.toPort)
  }

  def addLink(
      fromOp: PhysicalOp,
      fromPort: Int,
      toOp: PhysicalOp,
      toPort: Int
  ): PhysicalPlan = {
    val newOperators = operatorMap +
      (fromOp.id -> getOperator(fromOp.id).addOutput(toOp, fromPort, toPort)) +
      (toOp.id -> getOperator(toOp.id).addInput(fromOp, fromPort, toPort))

    this.copy(newOperators.values.toSet, links ++ Set(PhysicalLink(fromOp, fromPort, toOp, toPort)))
  }

  def removeLink(
      link: PhysicalLink
  ): PhysicalPlan = {
    val fromOpId = link.fromOp.id
    val toOpId = link.toOp.id
    val fromOp = getOperator(fromOpId)
    val toOp = getOperator(toOpId)
    val updatedFromOp = fromOp.removeOutput(link)
    val updatedToOp = toOp.removeInput(link)
    val newOperators = operatorMap +
      (fromOpId -> updatedFromOp) +
      (toOpId -> updatedToOp)
    this.copy(operators = newOperators.values.toSet, links.filter(l => l.id != link.id))
  }

  def setOperator(physicalOp: PhysicalOp): PhysicalPlan = {
    this.copy(operators = (operatorMap + (physicalOp.id -> physicalOp)).values.toSet)
  }

  def setOperatorUnblockPort(physicalOpId: PhysicalOpIdentity, portToRemove: Int): PhysicalPlan = {
    val physicalOp = getOperator(physicalOpId)
    physicalOp.copy(blockingInputs = physicalOp.blockingInputs.filter(port => port != portToRemove))
    this.copy(operators = operators ++ Set(physicalOp))
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
    links.filter(link => link.fromOp.id == from && link.toOp.id == to)

  }

  /**
    * This method will return an updated PhysicalPlan, with each PhysicalLink being
    * propagated with the partitioning, which varies based on the propagated partitioning
    * requirements from the upstream.
    *
    * For example, if the upstream of the link has the same partitioning requirement as
    * that of the downstream, and their number of workers are equal, then the partitioning
    * on this link can be optimized to OneToOne.
    */
  def populatePartitioningOnLinks(): PhysicalPlan = {
    val createdLinks = new mutable.ArrayBuffer[PhysicalLink]()
    // a map of an operator to its output partition info
    val outputPartitionInfos = new mutable.HashMap[PhysicalOpIdentity, PartitionInfo]()

    topologicalIterator()
      .foreach(physicalOpId => {
        val physicalOp = getOperator(physicalOpId)
        val outputPartitionInfo = if (getSourceOperatorIds.contains(physicalOpId)) {
          // get output partition info of the source operator
          physicalOp.partitionRequirement.headOption.flatten.getOrElse(UnknownPartition())
        } else {
          val inputPartitionings =
            enforcePartitionRequirement(physicalOp, outputPartitionInfos.toMap, createdLinks)
          assert(inputPartitionings.length == physicalOp.inputPorts.size)
          // derive the output partition info of this operator
          physicalOp.derivePartition(inputPartitionings.toList)
        }
        outputPartitionInfos.put(physicalOpId, outputPartitionInfo)
      })

    // returns the complete physical plan with link strategies
    this.copy(operators, createdLinks.toSet)
  }

  private def enforcePartitionRequirement(
      physicalOp: PhysicalOp,
      partitionInfos: Map[PhysicalOpIdentity, PartitionInfo],
      links: mutable.ArrayBuffer[PhysicalLink]
  ): Array[PartitionInfo] = {
    // for each input port, enforce partition requirement
    physicalOp.inputPorts.indices
      .map(port => {
        // all input PhysicalOpIds connected to this port
        val inputPhysicalOps = physicalOp.getOpsOnInputPort(port)

        val fromPort = getUpstreamPhysicalLinkIds(physicalOp.id).head.fromPort

        // the output partition info of each link connected from each input PhysicalOp
        // for each input PhysicalOp connected on this port
        // check partition requirement to enforce corresponding LinkStrategy
        val outputPartitions = inputPhysicalOps.map(inputPhysicalOp => {
          val inputPartitionInfo = partitionInfos(inputPhysicalOp.id)
          val (physicalLink, outputPart) =
            getOutputPartitionInfo(
              inputPhysicalOp.id,
              fromPort,
              physicalOp.id,
              port,
              inputPartitionInfo
            )
          links.append(physicalLink)
          outputPart
        })

        assert(outputPartitions.size == inputPhysicalOps.size)

        outputPartitions.reduce((a, b) => a.merge(b))
      })
      .toArray
  }

  private def getOutputPartitionInfo(
      fromPhysicalOpId: PhysicalOpIdentity,
      fromPort: Int,
      toPhysicalOpId: PhysicalOpIdentity,
      inputPort: Int,
      upstreamPartitionInfo: PartitionInfo
  ): (PhysicalLink, PartitionInfo) = {
    val toPhysicalOp = getOperator(toPhysicalOpId)
    val fromPhysicalOp = getOperator(fromPhysicalOpId)

    // make sure this input is connected to this port
    assert(toPhysicalOp.getOpsOnInputPort(inputPort).map(_.id).contains(fromPhysicalOpId))

    // partition requirement of this PhysicalOp on this input port
    val requiredPartitionInfo =
      toPhysicalOp.partitionRequirement.lift(inputPort).flatten.getOrElse(UnknownPartition())

    // the upstream partition info satisfies the requirement, and number of worker match
    if (
      upstreamPartitionInfo.satisfies(
        requiredPartitionInfo
      ) && fromPhysicalOp.getWorkerIds.length == toPhysicalOp.getWorkerIds.length
    ) {
      val physicalLink = new PhysicalLink(
        fromPhysicalOp,
        fromPort,
        toPhysicalOp,
        inputPort,
        partitionings = fromPhysicalOp.getWorkerIds.indices
          .map(i =>
            (
              OneToOnePartitioning(defaultBatchSize, List(toPhysicalOp.getWorkerIds(i))),
              List(toPhysicalOp.getWorkerIds(i))
            )
          )
          .toList
      )
      val outputPart = upstreamPartitionInfo
      (physicalLink, outputPart)
    } else {
      // we must re-distribute the input partitions
      val physicalLink =
        PhysicalLink(
          fromPhysicalOp,
          fromPort,
          toPhysicalOp,
          inputPort,
          requiredPartitionInfo
        )
      val outputPart = requiredPartitionInfo
      (physicalLink, outputPart)
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
                  link.fromOp.id == upstreamPhysicalOpId && link.toOp.id == physicalOp.id
                )
                .filter(link => getOperator(physicalOp.id).isInputLinkBlocking(link))
                .map(_.id)
            }
        }
      }

    this.copy(operators, links.filterNot(e => linksToRemove.contains(e.id)))
  }

  def areAllInputBlocking(physicalOpId: PhysicalOpIdentity): Boolean = {

    val upstreamPhysicalLinkIds = getUpstreamPhysicalLinkIds(physicalOpId)
    upstreamPhysicalLinkIds.nonEmpty && upstreamPhysicalLinkIds.forall { upstreamPhysicalLinkId =>
      getOperator(physicalOpId).isInputLinkBlocking(getLink(upstreamPhysicalLinkId))
    }
  }

}
