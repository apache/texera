package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LinkIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.operators.{OpExecConfig, SinkOpExecConfig}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan.toOutLinks
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.collection.JavaConverters._

object PhysicalPlan {

  def toOutLinks(
      allLinks: Iterable[(OperatorIdentity, OperatorIdentity)]
  ): Map[OperatorIdentity, Set[OperatorIdentity]] = {
    allLinks
      .groupBy(link => link._1)
      .mapValues(links => links.map(link => link._2).toSet)
  }

}

class PhysicalPlan(
    val workflowId: WorkflowIdentity,
    val operators: Map[OperatorIdentity, OpExecConfig],
    val outLinks: Map[OperatorIdentity, Set[OperatorIdentity]]
) {

  var pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = _

  lazy val dag: DirectedAcyclicGraph[OperatorIdentity, DefaultEdge] = {
    val jgraphtDag = new DirectedAcyclicGraph[OperatorIdentity, DefaultEdge](classOf[DefaultEdge])
    operators.foreach(op => jgraphtDag.addVertex(op._1))
    outLinks.foreach(pair => pair._2.foreach(dest => { jgraphtDag.addEdge(pair._1, dest) }))
    jgraphtDag
  }

  lazy val allOperatorIds: Iterable[OperatorIdentity] = operators.keys

  lazy val allLinks: List[(OperatorIdentity, OperatorIdentity)] =
    this.outLinks.flatten(o => o._2.map(dest => (o._1, dest))).toList

  lazy val sourceOperators: List[OperatorIdentity] =
    operators.keys.filter(op => dag.inDegreeOf(op) == 0).toList

  lazy val sinkOperators: List[OperatorIdentity] =
    operators.keys
      .filter(op => operators(op).isInstanceOf[SinkOpExecConfig])
      .toList

  def getSourceOperators: List[OperatorIdentity] = this.sourceOperators

  def getSinkOperators: List[OperatorIdentity] = this.sinkOperators

  def getUpstream(opID: OperatorIdentity): List[OperatorIdentity] = {
    dag.incomingEdgesOf(opID).asScala.map(e => dag.getEdgeSource(e)).toList
  }

  def getDownstream(opID: OperatorIdentity): List[OperatorIdentity] = {
    dag.outgoingEdgesOf(opID).asScala.map(e => dag.getEdgeTarget(e)).toList
  }

  def topologicalIterator(): Iterator[OperatorIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }
  // returns a new physical plan with the operators added
  def addOperator(opID: OperatorIdentity, opExecConfig: OpExecConfig): PhysicalPlan = {
    new PhysicalPlan(workflowId, operators + (opID -> opExecConfig), outLinks)
  }

  // returns a new physical plan with the edges added
  def addEdges(edgesToAdd: List[(OperatorIdentity, OperatorIdentity)]): PhysicalPlan = {
    for ((source, dest) <- edgesToAdd) {
      val layerLink = LinkIdentity(
        operators(source).topology.layers.last.id,
        operators(dest).topology.layers.head.id
      )
      // TODO: op exec config is not immutable yet
      // TODO: port is not fully supported in physical plan yet, only support single input/output port
      operators(source).outputToOrdinalMapping.update(layerLink, (0, ""))
      operators(dest).inputToOrdinalMapping.update(layerLink, (0, ""))
    }
    new PhysicalPlan(workflowId, operators, toOutLinks(allLinks ++ edgesToAdd))
  }

}
