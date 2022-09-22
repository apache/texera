package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.JavaConverters._

object PhysicalPlan {

  def apply(operatorList: Array[WorkerLayer], links: Array[LinkIdentity]): PhysicalPlan = {
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

class PhysicalPlan(
    val operatorList: List[WorkerLayer],
    val links: List[LinkIdentity]
) {

  var pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = _

  lazy val operators: Map[LayerIdentity, WorkerLayer] = operatorList.map(o => (o.id, o)).toMap

  lazy val dag: DirectedAcyclicGraph[LayerIdentity, DefaultEdge] = {
    val jgraphtDag = new DirectedAcyclicGraph[LayerIdentity, DefaultEdge](classOf[DefaultEdge])
    operators.foreach(op => jgraphtDag.addVertex(op._1))
    links.foreach(l => jgraphtDag.addEdge(l.from, l.to))
    jgraphtDag
  }

  lazy val allOperatorIds: Iterable[LayerIdentity] = operators.keys

  lazy val sourceOperators: List[LayerIdentity] =
    operators.keys.filter(op => dag.inDegreeOf(op) == 0).toList

  lazy val sinkOperators: List[LayerIdentity] =
    operators.keys
      .filter(op => dag.outDegreeOf(op) == 0)
      .toList

  def getSourceOperators: List[LayerIdentity] = this.sourceOperators

  def getSinkOperators: List[LayerIdentity] = this.sinkOperators

  def getUpstream(opID: LayerIdentity): List[LayerIdentity] = {
    dag.incomingEdgesOf(opID).asScala.map(e => dag.getEdgeSource(e)).toList
  }

  def getDownstream(opID: LayerIdentity): List[LayerIdentity] = {
    dag.outgoingEdgesOf(opID).asScala.map(e => dag.getEdgeTarget(e)).toList
  }

  def topologicalIterator(): Iterator[LayerIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }
  // returns a new physical plan with the operators added
  def addOperator(opID: LayerIdentity, opExecConfig: WorkerLayer): PhysicalPlan = {
    new PhysicalPlan(opExecConfig :: operatorList, links)
  }

  // returns a new physical plan with the edges added
  def addEdge(from: LayerIdentity, fromPort: Int, to: LayerIdentity, toPort: Int): PhysicalPlan = {
    val fromOp = operators(from).addOutput(to, toPort)
    val toOp = operators(to).addInput(from, fromPort)
    val newOperators = fromOp :: toOp :: operatorList
    val newLinks = links :+ LinkIdentity(from, to)
    new PhysicalPlan(newOperators, newLinks)
  }

}
