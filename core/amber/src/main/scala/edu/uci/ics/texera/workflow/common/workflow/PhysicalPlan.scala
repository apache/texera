package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
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

  def toOutLinks(
      allLinks: Iterable[(OperatorIdentity, OperatorIdentity)]
  ): Map[OperatorIdentity, Set[OperatorIdentity]] = {
    allLinks
      .groupBy(link => link._1)
      .mapValues(links => links.map(link => link._2).toSet)
  }

}

case class PhysicalPlan(
    operators: List[OpExecConfig],
    links: List[LinkIdentity],
    linkStrategies: Map[LinkIdentity, LinkStrategy] = Map(),
    pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = null
) {

  lazy val operatorMap: Map[LayerIdentity, OpExecConfig] = operators.map(o => (o.id, o)).toMap

  lazy val dag: DirectedAcyclicGraph[LayerIdentity, DefaultEdge] = {
    val jgraphtDag = new DirectedAcyclicGraph[LayerIdentity, DefaultEdge](classOf[DefaultEdge])
    operatorMap.foreach(op => jgraphtDag.addVertex(op._1))
    links.foreach(l => jgraphtDag.addEdge(l.from, l.to))
    jgraphtDag
  }

  lazy val allOperatorIds: Iterable[LayerIdentity] = operatorMap.keys

  lazy val sourceOperators: List[LayerIdentity] =
    operatorMap.keys.filter(op => dag.inDegreeOf(op) == 0).toList

  lazy val sinkOperators: List[LayerIdentity] =
    operatorMap.keys
      .filter(op => dag.outDegreeOf(op) == 0)
      .toList

  def getSourceOperators: List[LayerIdentity] = this.sourceOperators

  def getSinkOperators: List[LayerIdentity] = this.sinkOperators

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

  def getAllRegions(): List[PipelinedRegion] = {
    asScalaIterator(pipelinedRegionsDAG.iterator()).toList
  }

  def getOperatorsInRegion(region: PipelinedRegion): PhysicalPlan = {
    val newOpIds = region.getOperators()
    val newOps = operators.filter(op => newOpIds.contains(op.id))
    val newLinks = links.filter(l => newOpIds.contains(l.from) && newOpIds.contains(l.to))
    val newLinkStrategies = linkStrategies.filter(l => newLinks.contains(l._1))
    PhysicalPlan(newOps, newLinks, newLinkStrategies, pipelinedRegionsDAG)
  }

  // returns a new physical plan with the operators added
  def addOperator(opExecConfig: OpExecConfig): PhysicalPlan = {
    this.copy(operators = opExecConfig :: operators)
  }

  // returns a new physical plan with the edges added
  def addEdge(
      from: LayerIdentity,
      to: LayerIdentity,
      fromPort: Int = 0,
      toPort: Int = 0
  ): PhysicalPlan = {

    val newOperators = operatorMap +
      (from -> operatorMap(from).addOutput(to, fromPort)) +
      (to -> operatorMap(to).addInput(from, toPort))

    val newLinks = links :+ LinkIdentity(from, to)
    this.copy(operators = newOperators.values.toList, links = newLinks)
  }

  // returns a new physical plan with the edges removed
  def removeEdge(
      edge: LinkIdentity
  ): PhysicalPlan = {
    val from = edge.from
    val to = edge.to
    val newOperators = operatorMap +
      (from -> operatorMap(from).removeOutput(to)) +
      (to -> operatorMap(to).removeInput(from))

    val newLinks = links.filter(l => l != LinkIdentity(from, to))
    this.copy(operators = newOperators.values.toList, links = newLinks)
  }

  def setOperator(newOp: OpExecConfig): PhysicalPlan = {
    this.copy(operators = (operatorMap + (newOp.id -> newOp)).values.toList)
  }

}
