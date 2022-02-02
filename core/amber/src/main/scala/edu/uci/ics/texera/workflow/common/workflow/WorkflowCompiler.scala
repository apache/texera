package edu.uci.ics.texera.workflow.common.workflow

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{
  ConditionalGlobalBreakpoint,
  CountGlobalBreakpoint
}
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.AssignBreakpointHandler.AssignGlobalBreakpoint
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LinkIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator

import scala.collection.mutable

object WorkflowCompiler {

  def isSink(operatorID: String, workflowCompiler: WorkflowCompiler): Boolean = {
    val outLinks =
      workflowCompiler.workflowInfo.links.filter(link => link.origin.operatorID == operatorID)
    outLinks.isEmpty
  }

  def getUpstreamOperators(
      operatorID: String,
      workflowCompiler: WorkflowCompiler
  ): List[OperatorDescriptor] = {
    workflowCompiler.workflowInfo.links
      .filter(link => link.destination.operatorID == operatorID)
      .flatMap(link =>
        workflowCompiler.workflowInfo.operators.filter(o => o.operatorID == link.origin.operatorID)
      )
      .toList
  }

  class ConstraintViolationException(val violations: Map[String, Set[ConstraintViolation]])
      extends RuntimeException

}

class WorkflowCompiler(val workflowInfo: WorkflowInfo, val context: WorkflowContext) {
  workflowInfo.toDAG.operators.values.foreach(initOperator)

  def initOperator(operator: OperatorDescriptor): Unit = {
    operator.setContext(context)
  }

  def validate: Map[String, Set[ConstraintViolation]] =
    this.workflowInfo.operators
      .map(o => {
        o.operatorID -> {
          o.validate().toSet
        }
      })
      .toMap
      .filter(pair => pair._2.nonEmpty)

  def amberWorkflow(workflowId: WorkflowIdentity, opResultStorage: OpResultStorage): Workflow = {
    // pre-process: set output mode for sink based on the visualization operator before it
    workflowInfo.toDAG.getSinkOperators.foreach(sinkOpId => {
      val sinkOp = workflowInfo.toDAG.getOperator(sinkOpId)
      val upstream = workflowInfo.toDAG.getUpstream(sinkOpId)
      if (upstream.nonEmpty) {
        (upstream.head, sinkOp) match {
          // match the combination of a visualization operator followed by a sink operator
          case (viz: VisualizationOperator, sink: ProgressiveSinkOpDesc) =>
            sink.setOutputMode(viz.outputMode())
            sink.setChartType(viz.chartType())
          case _ =>
          //skip
        }
      }
    })

    val inputSchemaMap = propagateWorkflowSchema()
    val amberOperators: mutable.Map[OperatorIdentity, OpExecConfig] = mutable.Map()
    workflowInfo.operators.foreach(o => {
      val inputSchemas = inputSchemaMap(o).map(s => s.get).toArray
      val outputSchema = {
        if (o.isInstanceOf[SourceOperatorDescriptor]) o.getOutputSchema(Array())
        else o.getOutputSchema(inputSchemas)
      }
      // assign storage to texera-managed sinks before generating exec config
      o match {
        case sink: ProgressiveSinkOpDesc =>
          sink.getCachedUpstreamId match {
            case Some(upstreamId) =>
              sink.setStorage(opResultStorage.create(upstreamId, outputSchema))
            case None => sink.setStorage(opResultStorage.create(o.operatorID, outputSchema))
          }
        case _ =>
      }
      val amberOperator: OpExecConfig =
        o.operatorExecutor(OperatorSchemaInfo(inputSchemas, outputSchema))
      amberOperators.put(amberOperator.id, amberOperator)
    })

    val outLinks: mutable.Map[OperatorIdentity, mutable.Set[OperatorIdentity]] = mutable.Map()
    workflowInfo.links.foreach(link => {
      val origin = OperatorIdentity(this.context.jobId, link.origin.operatorID)
      val dest = OperatorIdentity(this.context.jobId, link.destination.operatorID)
      outLinks.getOrElseUpdate(origin, mutable.Set()).add(dest)

      val layerLink = LinkIdentity(
        amberOperators(origin).topology.layers.last.id,
        amberOperators(dest).topology.layers.head.id
      )
      amberOperators(dest).setInputToOrdinalMapping(layerLink, link.destination.portOrdinal)
    })

    val outLinksImmutable: Map[OperatorIdentity, Set[OperatorIdentity]] =
      outLinks.map({ case (operatorId, links) => operatorId -> links.toSet }).toMap

    new Workflow(workflowId, amberOperators, outLinksImmutable)
  }

  def propagateWorkflowSchema(): Map[OperatorDescriptor, List[Option[Schema]]] = {
    // a map from an operator to the list of its input schema
    val inputSchemaMap =
      new mutable.HashMap[OperatorDescriptor, mutable.MutableList[Option[Schema]]]()
        .withDefault(op => mutable.MutableList.fill(op.operatorInfo.inputPorts.size)(Option.empty))

    // propagate output schema following topological order
    val topologicalOrderIterator = workflowInfo.toDAG.jgraphtDag.iterator()
    topologicalOrderIterator.forEachRemaining(opID => {
      val op = workflowInfo.toDAG.getOperator(opID)
      // infer output schema of this operator based on its input schema
      val outputSchema: Option[Schema] = {
        // call to "getOutputSchema" might cause exceptions, wrap in try/catch and return empty schema
        try {
          if (op.isInstanceOf[SourceOperatorDescriptor]) {
            // op is a source operator, ask for it output schema
            Option.apply(op.getOutputSchema(Array()))
          } else if (!inputSchemaMap.contains(op) || inputSchemaMap(op).exists(s => s.isEmpty)) {
            // op does not have input, or any of the op's input's output schema is null
            // then this op's output schema cannot be inferred as well
            Option.empty
          } else {
            // op's input schema is complete, try to infer its output schema
            // if inference failed, print an exception message, but still continue the process
            Option.apply(op.getOutputSchema(inputSchemaMap(op).map(s => s.get).toArray))
          }
        } catch {
          case e: Throwable =>
            e.printStackTrace()
            Option.empty
        }
      }
      // exception: if op is a source operator, use its output schema as input schema for autocomplete
      if (op.isInstanceOf[SourceOperatorDescriptor]) {
        inputSchemaMap.update(op, mutable.MutableList(outputSchema))
      }

      // update input schema of all outgoing links
      val outLinks = this.workflowInfo.links.filter(link => link.origin.operatorID == op.operatorID)
      outLinks.foreach(link => {
        val dest = workflowInfo.operators.find(o => o.operatorID == link.destination.operatorID).get
        // get the input schema list, should be pre-populated with size equals to num of ports
        val destInputSchemas = inputSchemaMap(dest)
        // put the schema into the ordinal corresponding to the port
        destInputSchemas(link.destination.portOrdinal) = outputSchema
        inputSchemaMap.update(dest, destInputSchemas)
      })
    })

    inputSchemaMap
      .filter(e => !(e._2.exists(s => s.isEmpty) || e._2.isEmpty))
      .map(e => (e._1, e._2.toList))
      .toMap
  }

}
