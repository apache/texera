package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import org.jgrapht.graph.DirectedAcyclicGraph

import scala.collection.mutable

case class BreakpointInfo(operatorID: String, breakpoint: Breakpoint)

object WorkflowInfo {
  def toJgraphtDAG(workflowInfo: WorkflowInfo): DirectedAcyclicGraph[String, OperatorLink] = {
    val workflowDag =
      new DirectedAcyclicGraph[String, OperatorLink](classOf[OperatorLink])
    workflowInfo.operators.foreach(op => workflowDag.addVertex(op.operatorID))
    workflowInfo.links.foreach(l =>
      workflowDag.addEdge(
        l.origin.operatorID,
        l.destination.operatorID,
        l
      )
    )
    workflowDag
  }
}
case class WorkflowInfo(
    operators: mutable.MutableList[OperatorDescriptor],
    links: mutable.MutableList[OperatorLink],
    breakpoints: mutable.MutableList[BreakpointInfo]
) {
  var cachedOperatorIds: mutable.MutableList[String] = _

  private lazy val dag = new WorkflowDAG(this)

  def toDAG: WorkflowDAG = dag

  // helper class that converts the workflowInfo into a graph data structure
  class WorkflowDAG(workflowInfo: WorkflowInfo) {

    val operators: Map[String, OperatorDescriptor] =
      workflowInfo.operators.map(op => (op.operatorID, op)).toMap

    val jgraphtDag: DirectedAcyclicGraph[String, OperatorLink] =
      WorkflowInfo.toJgraphtDAG(workflowInfo)

    val sourceOperators: List[String] =
      operators.keys.filter(op => jgraphtDag.inDegreeOf(op) == 0).toList

    val sinkOperators: List[String] =
      operators.keys
        .filter(op => operators(op).isInstanceOf[SinkOpDesc])
        .toList

    def getOperator(operatorID: String): OperatorDescriptor = operators(operatorID)

    def getSourceOperators: List[String] = this.sourceOperators

    def getSinkOperators: List[String] = this.sinkOperators

    def getUpstream(operatorID: String): List[OperatorDescriptor] = {
      val upstream = new mutable.MutableList[OperatorDescriptor]
      jgraphtDag
        .incomingEdgesOf(operatorID)
        .forEach(e => upstream += operators(e.origin.operatorID))
      upstream.toList
    }

    def getDownstream(operatorID: String): List[OperatorDescriptor] = {
      val downstream = new mutable.MutableList[OperatorDescriptor]
      jgraphtDag
        .outgoingEdgesOf(operatorID)
        .forEach(e => downstream += operators(e.destination.operatorID))
      downstream.toList
    }

  }
}
