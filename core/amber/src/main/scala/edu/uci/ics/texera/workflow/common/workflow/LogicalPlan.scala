package edu.uci.ics.texera.workflow.common.workflow

import com.google.common.base.Verify
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
import org.jgrapht.graph.DirectedAcyclicGraph

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConverters, mutable}

case class BreakpointInfo(operatorID: String, breakpoint: Breakpoint)

object LogicalPlan {

  private def toJgraphtDAG(
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

  def apply(
      pojo: LogicalPlanPojo,
      ctx: WorkflowContext
  ): LogicalPlan = {
    LogicalPlan(ctx, pojo.operators, pojo.links, pojo.breakpoints)
  }

}

case class LogicalPlan(
    context: WorkflowContext,
    operators: List[OperatorDescriptor],
    links: List[OperatorLink],
    breakpoints: List[BreakpointInfo],
    inputSchemaMap: Map[OperatorIdentity, List[Option[Schema]]] = Map.empty
) extends LazyLogging {

  lazy val operatorMap: Map[String, OperatorDescriptor] =
    operators.map(op => (op.operatorID, op)).toMap

  lazy val jgraphtDag: DirectedAcyclicGraph[String, OperatorLink] =
    LogicalPlan.toJgraphtDAG(operators, links)

  lazy val sourceOperators: List[String] =
    operatorMap.keys.filter(op => jgraphtDag.inDegreeOf(op) == 0).toList

  lazy val terminalOperators: List[String] =
    operatorMap.keys
      .filter(op => jgraphtDag.outDegreeOf(op) == 0)
      .toList

  lazy val outputSchemaMap: Map[OperatorIdentity, List[Schema]] =
    operatorMap.values
      .map(o => {
        val inputSchemas: Array[Schema] =
          if (!operatorMap(o.operatorID).isInstanceOf[SourceOperatorDescriptor])
            inputSchemaMap(o.operatorIdentifier).map(s => s.get).toArray
          else Array()
        val outputSchemas = o.getOutputSchemas(inputSchemas).toList
        (o.operatorIdentifier, outputSchemas)
      })
      .toMap

  def getOperator(operatorID: String): OperatorDescriptor = operatorMap(operatorID)

  def getSourceOperators: List[String] = this.sourceOperators

  def getTerminalOperators: List[String] = this.terminalOperators

  def getAncestorOpIds(operatorID: String): Set[String] = {
    JavaConverters.asScalaSet(jgraphtDag.getAncestors(operatorID)).toSet
  }

  def getUpstream(operatorID: String): List[OperatorDescriptor] = {
    val upstream = new mutable.MutableList[OperatorDescriptor]
    jgraphtDag
      .incomingEdgesOf(operatorID)
      .forEach(e => upstream += operatorMap(e.origin.operatorID))
    upstream.toList
  }

  // returns a new logical plan with the given operator added
  def addOperator(operatorDescriptor: OperatorDescriptor): LogicalPlan = {
    // TODO: fix schema for the new operator
    this.copy(context, operators :+ operatorDescriptor, links, breakpoints)
  }

  def removeOperator(operatorId: String): LogicalPlan = {
    this.copy(
      context,
      operators.filter(o => o.operatorID != operatorId),
      links.filter(l =>
        l.origin.operatorID != operatorId && l.destination.operatorID != operatorId
      ),
      breakpoints.filter(b => b.operatorID != operatorId),
      inputSchemaMap.filter({
        case (opId, schemas) => opId.operator != operatorId
      })
    )
  }

  // returns a new logical plan with the given edge added
  def addEdge(
      from: String,
      to: String,
      fromPort: Int = 0,
      toPort: Int = 0
  ): LogicalPlan = {
    val newLink = OperatorLink(OperatorPort(from, fromPort), OperatorPort(to, toPort))
    val newLinks = links :+ newLink
    this.copy(context, operators, newLinks, breakpoints)
  }

  // returns a new logical plan with the given edge removed
  def removeEdge(
      from: String,
      to: String,
      fromPort: Int = 0,
      toPort: Int = 0
  ): LogicalPlan = {
    val linkToRemove = OperatorLink(OperatorPort(from, fromPort), OperatorPort(to, toPort))
    val newLinks = links.filter(l => l != linkToRemove)
    this.copy(context, operators, newLinks, breakpoints)
  }

  def getDownstream(operatorID: String): List[OperatorDescriptor] = {
    val downstream = new mutable.MutableList[OperatorDescriptor]
    jgraphtDag
      .outgoingEdgesOf(operatorID)
      .forEach(e => downstream += operatorMap(e.destination.operatorID))
    downstream.toList
  }

  def getDownstreamEdges(operatorID: String): List[OperatorLink] = {
    links.filter(l => l.origin.operatorID == operatorID)
  }

  def opSchemaInfo(operatorID: String): OperatorSchemaInfo = {
    val op = operatorMap(operatorID)
    val inputSchemas: Array[Schema] =
      if (!op.isInstanceOf[SourceOperatorDescriptor])
        inputSchemaMap(op.operatorIdentifier).map(s => s.get).toArray
      else Array()
    val outputSchemas = outputSchemaMap(op.operatorIdentifier).toArray
    OperatorSchemaInfo(inputSchemas, outputSchemas)
  }

  def propagateWorkflowSchema(
      errorList: Option[ArrayBuffer[(String, Throwable)]]
  ): LogicalPlan = {

    operators.foreach(operator => {
      if (operator.context == null) {
        operator.setContext(context)
      }
    })

    // a map from an operator to the list of its input schema
    val inputSchemaMap =
      new mutable.HashMap[OperatorIdentity, mutable.MutableList[Option[Schema]]]()
        .withDefault(op =>
          mutable.MutableList
            .fill(operatorMap(op.operator).operatorInfo.inputPorts.size)(Option.empty)
        )
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
          } else if (
            !inputSchemaMap.contains(op.operatorIdentifier) || inputSchemaMap(op.operatorIdentifier)
              .exists(s => s.isEmpty)
          ) {
            // op does not have input, or any of the op's input's output schema is null
            // then this op's output schema cannot be inferred as well
            Option.empty
          } else {
            // op's input schema is complete, try to infer its output schema
            // if inference failed, print an exception message, but still continue the process
            Option.apply(
              op.getOutputSchemas(inputSchemaMap(op.operatorIdentifier).map(s => s.get).toArray)
            )
          }
        } catch {
          case e: Throwable =>
            logger.error("got error", e)
            errorList match {
              case Some(list) => list.append((opID, e))
              case None       =>
            }

            Option.empty
        }
      }
      // exception: if op is a source operator, use its output schema as input schema for autocomplete
      if (op.isInstanceOf[SourceOperatorDescriptor]) {
        inputSchemaMap.update(
          op.operatorIdentifier,
          mutable.MutableList(outputSchemas.map(s => s(0)))
        )
      }

      if (!op.isInstanceOf[SinkOpDesc] && outputSchemas.nonEmpty) {
        Verify.verify(outputSchemas.get.length == op.operatorInfo.outputPorts.length)
      }

      // update input schema of all outgoing links
      val outLinks = links.filter(link => link.origin.operatorID == op.operatorID)
      outLinks.foreach(link => {
        val dest = operatorMap(link.destination.operatorID)
        // get the input schema list, should be pre-populated with size equals to num of ports
        val destInputSchemas = inputSchemaMap(dest.operatorIdentifier)
        // put the schema into the ordinal corresponding to the port
        val schemaOnPort =
          outputSchemas.flatMap(schemas => schemas.toList.lift(link.origin.portOrdinal))
        destInputSchemas(link.destination.portOrdinal) = schemaOnPort
        inputSchemaMap.update(dest.operatorIdentifier, destInputSchemas)
      })
    })

    this.copy(
      context,
      operators,
      links,
      breakpoints,
      inputSchemaMap
        .filter({
          case (_: OperatorIdentity, schemas: mutable.MutableList[Option[Schema]]) =>
            !(schemas.exists(s => s.isEmpty) || schemas.isEmpty)
        })
        .map({ // we need to convert to immutable data structures
          case (opId: OperatorIdentity, schemas: mutable.MutableList[Option[Schema]]) =>
            (opId, schemas.toList)
        })
        .toMap
    )
  }

}
