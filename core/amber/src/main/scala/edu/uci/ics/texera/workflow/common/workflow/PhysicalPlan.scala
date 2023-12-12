package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.virtualidentity.util.toOperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.JavaConverters._

object PhysicalPlan {

  def apply(operatorList: Array[OpExecConfig], links: Array[LinkIdentity]): PhysicalPlan = {
    new PhysicalPlan(operatorList.toList, links.toList)
  }

  def apply(executionId: Long, logicalPlan: LogicalPlan): PhysicalPlan = {

    var physicalPlan = PhysicalPlan(List(), List())

    logicalPlan.operators.foreach(op => {
      val subPlan =
        op.operatorExecutorMultiLayer(
          executionId,
          logicalPlan.getOpSchemaInfo(op.operatorIdentifier)
        )
      physicalPlan = physicalPlan.addSubPlan(subPlan)
    })

    // connect inter-operator links
    logicalPlan.links.foreach(link => {
      val fromLogicalOp = logicalPlan.getOperator(link.origin.operatorId).operatorIdentifier
      val fromPort = link.origin.portOrdinal
      val fromPortName = logicalPlan.operators
        .filter(op => op.operatorIdentifier == link.origin.operatorId)
        .head
        .operatorInfo
        .outputPorts(fromPort)
        .displayName
      val fromLayer = physicalPlan.findLayerForOutputPort(fromLogicalOp, fromPortName)

      val toLogicalOp = logicalPlan.getOperator(link.destination.operatorId).operatorIdentifier
      val toPort = link.destination.portOrdinal
      val toPortName = logicalPlan.operators
        .filter(op => op.operatorIdentifier == link.destination.operatorId)
        .head
        .operatorInfo
        .inputPorts(toPort)
        .displayName
      val toLayer = physicalPlan.findLayerForInputPort(toLogicalOp, toPortName)

      physicalPlan = physicalPlan.addEdge(fromLayer, fromPort, toLayer, toPort)
    })

    physicalPlan
  }

}

case class PhysicalPlan(
    operators: List[OpExecConfig],
    links: List[LinkIdentity]
) {

  @transient lazy val operatorMap: Map[LayerIdentity, OpExecConfig] =
    operators.map(o => (o.id, o)).toMap

  // the dag will be re-computed again once it reaches the coordinator.
  @transient lazy val dag: DirectedAcyclicGraph[LayerIdentity, DefaultEdge] = {
    val jgraphtDag = new DirectedAcyclicGraph[LayerIdentity, DefaultEdge](classOf[DefaultEdge])
    operatorMap.foreach(op => jgraphtDag.addVertex(op._1))
    links.foreach(l => jgraphtDag.addEdge(l.from, l.to))
    jgraphtDag
  }

  @transient lazy val allOperatorIds: Iterable[LayerIdentity] = operatorMap.keys

  @transient lazy val sourceOperators: List[LayerIdentity] =
    operatorMap.keys.filter(op => dag.inDegreeOf(op) == 0).toList

  @transient lazy val sinkOperators: List[LayerIdentity] =
    operatorMap.keys
      .filter(op => dag.outDegreeOf(op) == 0)
      .toList

  def getSourceOperators: List[LayerIdentity] = this.sourceOperators

  def getSinkOperators: List[LayerIdentity] = this.sinkOperators

  private def findLayerForInputPort(opName: OperatorIdentity, portName: String): LayerIdentity = {
    val candidateLayers = layersOfLogicalOperator(opName).filter(op =>
      op.inputPorts.map(_.displayName).contains(portName)
    )
    assert(
      candidateLayers.size == 1,
      s"find no or multiple input port with name = $portName for operator $opName"
    )
    candidateLayers.head.id
  }

  def findLayerForOutputPort(opName: OperatorIdentity, portName: String): LayerIdentity = {
    val candidateLayers = layersOfLogicalOperator(opName).filter(op =>
      op.outputPorts.map(_.displayName).contains(portName)
    )
    assert(
      candidateLayers.size == 1,
      s"find no or multiple output port with name = $portName for operator $opName"
    )
    candidateLayers.head.id
  }

  def layersOfLogicalOperator(opId: OperatorIdentity): List[OpExecConfig] = {
    topologicalIterator()
      .filter(layerId => toOperatorIdentity(layerId) == opId)
      .map(layerId => getLayer(layerId))
      .toList
  }

  def getSingleLayerOfLogicalOperator(opId: OperatorIdentity): OpExecConfig = {
    val ops = layersOfLogicalOperator(opId)
    if (ops.size != 1) {
      val msg = s"operator $opId has ${ops.size} physical operators, expecting a single one"
      throw new RuntimeException(msg)
    }
    ops.head
  }

  // returns a sub-plan that contains the specified operators and the links connected within these operators
  def subPlan(subOperators: Set[LayerIdentity]): PhysicalPlan = {
    val newOps = operators.filter(op => subOperators.contains(op.id))
    val newLinks =
      links.filter(link => subOperators.contains(link.from) && subOperators.contains(link.to))
    PhysicalPlan(newOps, newLinks)
  }

  def getLayer(layer: LayerIdentity): OpExecConfig = operatorMap(layer)

  def getUpstream(opID: LayerIdentity): List[LayerIdentity] = {
    dag.incomingEdgesOf(opID).asScala.map(e => dag.getEdgeSource(e)).toList
  }

  def getUpstreamLinks(opID: LayerIdentity): List[LinkIdentity] = {
    links.filter(l => l.to == opID)
  }

  def getDownstream(opID: LayerIdentity): List[LayerIdentity] = {
    dag.outgoingEdgesOf(opID).asScala.map(e => dag.getEdgeTarget(e)).toList
  }

  def getDescendants(opID: LayerIdentity): List[LayerIdentity] = {
    dag.getDescendants(opID).asScala.toList
  }

  def topologicalIterator(): Iterator[LayerIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }

  // returns a new physical plan with the operators added
  def addOperator(opExecConfig: OpExecConfig): PhysicalPlan = {
    this.copy(operators = opExecConfig :: operators)
  }

  def addEdge(
      from: LayerIdentity,
      fromPort: Int,
      to: LayerIdentity,
      toPort: Int
  ): PhysicalPlan = {

    val newOperators = operatorMap +
      (from -> operatorMap(from).addOutput(to, fromPort, toPort)) +
      (to -> operatorMap(to).addInput(from, fromPort, toPort))

    val newLinks = links :+ LinkIdentity(from, fromPort, to, toPort)
    this.copy(operators = newOperators.values.toList, links = newLinks)
  }

  // returns a new physical plan with the edges removed
  def removeEdge(
      edge: LinkIdentity
  ): PhysicalPlan = {
    val from = edge.from
    val to = edge.to
    val newOperators = operatorMap +
      (from -> operatorMap(from).removeOutput(edge)) +
      (to -> operatorMap(to).removeInput(edge))

    val newLinks = links.filter(l => l != edge)
    this.copy(operators = newOperators.values.toList, links = newLinks)
  }

  def setOperator(newOp: OpExecConfig): PhysicalPlan = {
    this.copy(operators = (operatorMap + (newOp.id -> newOp)).values.toList)
  }

  private def addSubPlan(subPlan: PhysicalPlan): PhysicalPlan = {
    var resultPlan = this.copy(operators, links)
    // add all physical operators to physical DAG
    subPlan.operators.foreach(op => resultPlan = resultPlan.addOperator(op))
    // connect intra-operator links
    subPlan.links.foreach((l: LinkIdentity) =>
      resultPlan = resultPlan.addEdge(l.from, l.fromPort, l.to, l.toPort)
    )
    resultPlan
  }

}
