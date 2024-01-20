package edu.uci.ics.texera.workflow.common.workflow

import com.google.common.base.Verify
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
import org.jgrapht.graph.DirectedAcyclicGraph

import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConverters, mutable}

case class BreakpointInfo(operatorID: String, breakpoint: Breakpoint)

object LogicalPlan {

  private def toJgraphtDAG(
      operatorList: List[LogicalOp],
      links: List[LogicalLink]
  ): DirectedAcyclicGraph[OperatorIdentity, LogicalLink] = {
    val workflowDag =
      new DirectedAcyclicGraph[OperatorIdentity, LogicalLink](classOf[LogicalLink])
    operatorList.foreach(op => workflowDag.addVertex(op.operatorIdentifier))
    links.foreach(l =>
      workflowDag.addEdge(
        l.fromOpId,
        l.toOpId,
        l
      )
    )
    workflowDag
  }

  def apply(
      pojo: LogicalPlanPojo
  ): LogicalPlan = {
    LogicalPlan(pojo.operators, pojo.links, pojo.breakpoints)
  }

}

case class LogicalPlan(
    operators: List[LogicalOp],
    links: List[LogicalLink],
    breakpoints: List[BreakpointInfo],
    inputSchemaMap: Map[OperatorIdentity, List[Option[Schema]]] = Map.empty
) extends LazyLogging {

  private lazy val operatorMap: Map[OperatorIdentity, LogicalOp] =
    operators.map(op => (op.operatorIdentifier, op)).toMap

  private lazy val jgraphtDag: DirectedAcyclicGraph[OperatorIdentity, LogicalLink] =
    LogicalPlan.toJgraphtDAG(operators, links)

  private lazy val outputSchemaMap: Map[OperatorIdentity, List[Schema]] =
    operatorMap.values
      .map(op => {
        val inputSchemas: Array[Schema] =
          if (!op.isInstanceOf[SourceOperatorDescriptor])
            inputSchemaMap(op.operatorIdentifier).map(s => s.get).toArray
          else Array()
        val outputSchemas = op.getOutputSchemas(inputSchemas).toList
        (op.operatorIdentifier, outputSchemas)
      })
      .toMap
  def getTopologicalOpIds: util.Iterator[OperatorIdentity] = jgraphtDag.iterator()

  def getOperator(opId: String): LogicalOp = operatorMap(OperatorIdentity(opId))

  def getOperator(opId: OperatorIdentity): LogicalOp = operatorMap(opId)

  def getSourceOperatorIds: List[OperatorIdentity] =
    operatorMap.keys.filter(op => jgraphtDag.inDegreeOf(op) == 0).toList

  def getTerminalOperatorIds: List[OperatorIdentity] =
    operatorMap.keys
      .filter(op => jgraphtDag.outDegreeOf(op) == 0)
      .toList

  def getAncestorOpIds(opId: OperatorIdentity): Set[OperatorIdentity] = {
    JavaConverters.asScalaSet(jgraphtDag.getAncestors(opId)).toSet
  }

  def getUpstreamOps(opId: OperatorIdentity): List[LogicalOp] = {
    jgraphtDag
      .incomingEdgesOf(opId)
      .map(e => operatorMap(e.fromOpId))
      .toList
  }

  def addOperator(op: LogicalOp): LogicalPlan = {
    // TODO: fix schema for the new operator
    this.copy(operators :+ op, links, breakpoints)
  }

  def removeOperator(opId: OperatorIdentity): LogicalPlan = {
    this.copy(
      operators.filter(o => o.operatorIdentifier != opId),
      links.filter(l => l.fromOpId != opId && l.toOpId != opId),
      breakpoints.filter(b => OperatorIdentity(b.operatorID) != opId),
      inputSchemaMap.filter({
        case (operatorId, _) => operatorId != opId
      })
    )
  }

  def addLink(
      fromOpId: OperatorIdentity,
      fromPortId: PortIdentity,
      toOpId: OperatorIdentity,
      toPortId: PortIdentity
  ): LogicalPlan = {
    val newLink = LogicalLink(
      fromOpId,
      fromPortId,
      toOpId,
      toPortId
    )
    val newLinks = links :+ newLink
    this.copy(operators, newLinks, breakpoints)
  }

  def removeLink(linkToRemove: LogicalLink): LogicalPlan = {
    this.copy(operators, links.filter(l => l != linkToRemove), breakpoints)
  }

  def getDownstreamOps(opId: OperatorIdentity): List[LogicalOp] = {
    val downstream = new mutable.MutableList[LogicalOp]
    jgraphtDag
      .outgoingEdgesOf(opId)
      .forEach(e => downstream += operatorMap(e.toOpId))
    downstream.toList
  }

  def getDownstreamLinks(opId: OperatorIdentity): List[LogicalLink] = {
    links.filter(l => l.fromOpId == opId)
  }

  def getOpInputSchemas(opId: OperatorIdentity): List[Option[Schema]] = {
    inputSchemaMap(opId)
  }
  def getOpOutputSchemas(opId: OperatorIdentity): List[Schema] = {
    outputSchemaMap(opId)
  }

  def getOpSchemaInfo(opId: OperatorIdentity): OperatorSchemaInfo = {
    val op = getOperator(opId)
    val inputSchemas: Array[Schema] =
      if (op.isInstanceOf[SourceOperatorDescriptor]) {
        Array() // source ops have no input schema
      } else { getOpInputSchemas(op.operatorIdentifier).map(s => s.get).toArray }
    val outputSchemas = getOpOutputSchemas(opId).toArray
    OperatorSchemaInfo(inputSchemas, outputSchemas)
  }

  def propagateWorkflowSchema(
      context: WorkflowContext,
      errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]]
  ): LogicalPlan = {

    operators.foreach(operator => {
      if (operator.getContext == null) {
        operator.setContext(context)
      }
    })

    // a map from an operator to the list of its input schema
    val inputSchemaMap =
      new mutable.HashMap[OperatorIdentity, mutable.MutableList[Option[Schema]]]()
        .withDefault(opId =>
          mutable.MutableList
            .fill(getOperator(opId).operatorInfo.inputPorts.size)(Option.empty)
        )
    // propagate output schema following topological order
    val topologicalOrderIterator = jgraphtDag.iterator()
    topologicalOrderIterator.forEachRemaining(opId => {
      val op = getOperator(opId)
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
              case Some(list) => list.append((opId, e))
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
      val outLinks = links.filter(link => link.fromOpId == op.operatorIdentifier)
      outLinks.foreach(link => {
        val dest = getOperator(link.toOpId)
        // get the input schema list, should be pre-populated with size equals to num of ports
        val destInputSchemas = inputSchemaMap(dest.operatorIdentifier)
        // put the schema into the ordinal corresponding to the port
        val schemaOnPort =
          outputSchemas.flatMap(schemas => schemas.toList.lift(link.fromPortId.id))
        destInputSchemas(link.toPortId.id) = schemaOnPort
        inputSchemaMap.update(dest.operatorIdentifier, destInputSchemas)
      })
    })

    this.copy(
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
