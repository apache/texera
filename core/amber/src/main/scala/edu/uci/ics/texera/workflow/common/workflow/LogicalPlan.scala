package edu.uci.ics.texera.workflow.common.workflow

import com.google.common.base.Verify
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import org.jgrapht.graph.DirectedAcyclicGraph

import scala.collection.mutable

case class BreakpointInfo(operatorID: String, breakpoint: Breakpoint)

object LogicalPlan {
  def toJgraphtDAG(
      operatorList: List[OperatorDescriptor],
      links: List[OperatorLink]
  ): DirectedAcyclicGraph[String, OperatorLink] = {
    val workflowDag =
      new DirectedAcyclicGraph[String, OperatorLink](classOf[OperatorLink])
    operatorList.foreach(op => workflowDag.addVertex(op.operatorID))
    links.foreach(l =>
      workflowDag.addEdge(
        l.origin.operatorID,
        l.destination.operatorID,
        l
      )
    )
    workflowDag
  }
}
class LogicalPlan(
    val operatorList: List[OperatorDescriptor],
    val links: List[OperatorLink],
    val breakpoints: List[BreakpointInfo]
) {

  var cachedOperatorIds: List[String] = _

  val operators: Map[String, OperatorDescriptor] =
    operatorList.map(op => (op.operatorID, op)).toMap

  lazy val jgraphtDag: DirectedAcyclicGraph[String, OperatorLink] =
    LogicalPlan.toJgraphtDAG(operatorList, links)

  lazy val sourceOperators: List[String] =
    operators.keys.filter(op => jgraphtDag.inDegreeOf(op) == 0).toList

  lazy val sinkOperators: List[String] =
    operators.keys
      .filter(op => operators(op).isInstanceOf[SinkOpDesc])
      .toList

  lazy val inputSchemaMap: Map[OperatorDescriptor, List[Option[Schema]]] =
    propagateWorkflowSchema()

  lazy val outputSchemaMap: Map[String, List[Schema]] =
    operators.values.map(o => {
      val inputSchemas: Array[Schema] =
        if (!o.isInstanceOf[SourceOperatorDescriptor])
          inputSchemaMap(o).map(s => s.get).toArray
        else Array()
      val outputSchemas = o.getOutputSchemas(inputSchemas).toList
      (o.operatorID, outputSchemas)
    }).toMap

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

  def propagateWorkflowSchema(): Map[OperatorDescriptor, List[Option[Schema]]] = {
    // a map from an operator to the list of its input schema
    val inputSchemaMap =
      new mutable.HashMap[OperatorDescriptor, mutable.MutableList[Option[Schema]]]()
        .withDefault(op => mutable.MutableList.fill(op.operatorInfo.inputPorts.size)(Option.empty))

    // propagate output schema following topological order
    val topologicalOrderIterator = jgraphtDag.iterator()
    topologicalOrderIterator.forEachRemaining(opID => {
      val op = getOperator(opID)
      // infer output schema of this operator based on its input schema
      val outputSchemas: Option[Array[Schema]] = {
        // call to "getOutputSchema" might cause exceptions, wrap in try/catch and return empty schema
        try {
          if (op.isInstanceOf[SourceOperatorDescriptor]) {
            // op is a source operator, ask for it output schema
            Option.apply(op.getOutputSchemas(Array()))
          } else if (!inputSchemaMap.contains(op) || inputSchemaMap(op).exists(s => s.isEmpty)) {
            // op does not have input, or any of the op's input's output schema is null
            // then this op's output schema cannot be inferred as well
            Option.empty
          } else {
            // op's input schema is complete, try to infer its output schema
            // if inference failed, print an exception message, but still continue the process
            Option.apply(op.getOutputSchemas(inputSchemaMap(op).map(s => s.get).toArray))
          }
        } catch {
          case e: Throwable =>
            e.printStackTrace()
            Option.empty
        }
      }
      // exception: if op is a source operator, use its output schema as input schema for autocomplete
      if (op.isInstanceOf[SourceOperatorDescriptor]) {
        inputSchemaMap.update(op, mutable.MutableList(outputSchemas.map(s => s(0))))
      }

      if (!op.isInstanceOf[SinkOpDesc] && outputSchemas.nonEmpty) {
        Verify.verify(outputSchemas.get.length == op.operatorInfo.outputPorts.length)
      }

      // update input schema of all outgoing links
      val outLinks = links.filter(link => link.origin.operatorID == op.operatorID)
      outLinks.foreach(link => {
        val dest = operators(link.destination.operatorID)
        // get the input schema list, should be pre-populated with size equals to num of ports
        val destInputSchemas = inputSchemaMap(dest)
        // put the schema into the ordinal corresponding to the port
        val schemaOnPort =
          outputSchemas.flatMap(schemas => schemas.toList.lift(link.origin.portOrdinal))
        destInputSchemas(link.destination.portOrdinal) = schemaOnPort
        inputSchemaMap.update(dest, destInputSchemas)
      })
    })

    inputSchemaMap
      .filter(e => !(e._2.exists(s => s.isEmpty) || e._2.isEmpty))
      .map(e => (e._1, e._2.toList))
      .toMap
  }

  def toPhysicalPlan(
      context: WorkflowContext,
      opResultStorage: OpResultStorage
  ): PhysicalPlan = {
//    val amberOperators: mutable.Map[LayerIdentity, WorkerLayer] = mutable.Map()
//    val amberLinks: mutable.MutableList[LinkIdentity] = mutable.MutableList()
//    val physicalOperators: mutable.Map[String, WorkerLayer] = mutable.Map()

    // assign storage to texera-managed sinks before generating exec config
    operatorList.foreach {
      case o@(sink: ProgressiveSinkOpDesc) =>
        sink.getCachedUpstreamId match {
          case Some(upstreamId) =>
            sink.setStorage(opResultStorage.create(upstreamId, outputSchemaMap(o.operatorID).head))
          case None => sink.setStorage(opResultStorage.create(o.operatorID, outputSchemaMap(o.operatorID).head))
        }
      case _ =>
    }

    var physicalPlan = PhysicalPlan(List(), List())

    operatorList.foreach(o => {
      val inputSchemas: Array[Schema] =
        if (!o.isInstanceOf[SourceOperatorDescriptor])
          inputSchemaMap(o).map(s => s.get).toArray
        else Array()
      val outputSchemas = outputSchemaMap(o.operatorID).toArray
      val smallPhysicalPlan =
        o.operatorExecutorMultiLayer(OperatorSchemaInfo(inputSchemas, outputSchemas))

      smallPhysicalPlan.operatorList.foreach(op => physicalPlan = physicalPlan.addOperator(op))
      // connect intra-operator links
      smallPhysicalPlan.links.foreach(l => physicalPlan = physicalPlan.addEdge(l.from, l.to))

    })

//    // connect inter-operator links
//    links.foreach(link => {
//      physicalPlan = physicalPlan.addEdge()
//    })

    // update the input and output port maps of OpExecConfigs with the link identities
//    val outLinks: mutable.Map[OperatorIdentity, Set[OperatorIdentity]] = mutable.Map()
//    links.foreach(link => {
//      val origin = OperatorIdentity(context.jobId, link.origin.operatorID)
//      val dest = OperatorIdentity(context.jobId, link.destination.operatorID)
//      outLinks.update(origin, outLinks.getOrElse(origin, Set()) + dest)
//
//      val layerLink = LinkIdentity(
//        amberOperators(origin).topology.layers.last.id,
//        amberOperators(dest).topology.layers.head.id
//      )
//      amberOperators(dest).setInputToOrdinalMapping(
//        layerLink,
//        link.destination.portOrdinal,
//        link.destination.portName
//      )
//      amberOperators(origin)
//        .setOutputToOrdinalMapping(layerLink, link.origin.portOrdinal, link.origin.portName)
//    })

//    new PhysicalPlan(amberOperators.values.toList, amberLinks.toList)
    null
  }

}
