package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowPipelinedRegionsBuilder
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LinkIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator

import scala.collection.mutable

object WorkflowCompiler {

  def isSink(operatorID: String, workflowCompiler: WorkflowCompiler): Boolean = {
    val outLinks =
      workflowCompiler.logicalPlan.links.filter(link => link.origin.operatorID == operatorID)
    outLinks.isEmpty
  }

  class ConstraintViolationException(val violations: Map[String, Set[ConstraintViolation]])
      extends RuntimeException

}

class WorkflowCompiler(val logicalPlan: LogicalPlan, val context: WorkflowContext) {
  logicalPlan.operatorMap.values.foreach(initOperator)

  def initOperator(operator: OperatorDescriptor): Unit = {
    operator.setContext(context)
  }

  def validate: Map[String, Set[ConstraintViolation]] =
    this.logicalPlan.operatorMap
      .map(o => (o._1, o._2.validate().toSet))
      .filter(o => o._2.nonEmpty)

  def amberWorkflow(workflowId: WorkflowIdentity, opResultStorage: OpResultStorage): Workflow = {
    // pre-process: set output mode for sink based on the visualization operator before it
    logicalPlan.getSinkOperators.foreach(sinkOpId => {
      val sinkOp = logicalPlan.getOperator(sinkOpId)
      val upstream = logicalPlan.getUpstream(sinkOpId)
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

    // create and save OpExecConfigs for the operators
    val (inputSchemaMap, errorList) = logicalPlan.propagateWorkflowSchema()
    if (errorList.nonEmpty) {
      throw new RuntimeException(
        s"${errorList.size} error(s) occurred in workflow submission: \n ${errorList.mkString("\n")}"
      )
    }
    val amberOperators: mutable.Map[OperatorIdentity, OpExecConfig] = mutable.Map()
    val amberOperatorsCopy: mutable.Map[OperatorIdentity, OpExecConfig] = mutable.Map()
    logicalPlan.operators.foreach(o => {
      val inputSchemas: Array[Schema] =
        if (!o.isInstanceOf[SourceOperatorDescriptor])
          inputSchemaMap(o.operatorIdentifier).map(s => s.get).toArray
        else Array()
      val outputSchemas = o.getOutputSchemas(inputSchemas)
      // assign storage to texera-managed sinks before generating exec config
      o match {
        case sink: ProgressiveSinkOpDesc =>
          sink.getCachedUpstreamId match {
            case Some(upstreamId) =>
              sink.setStorage(opResultStorage.create(upstreamId, outputSchemas(0)))
            case None => sink.setStorage(opResultStorage.create(o.operatorID, outputSchemas(0)))
          }
        case _ =>
      }
      val amberOperator: OpExecConfig =
        o.operatorExecutor(OperatorSchemaInfo(inputSchemas, outputSchemas))
      val amberOperatorCopy: OpExecConfig =
        o.operatorExecutor(OperatorSchemaInfo(inputSchemas, outputSchemas))
      amberOperators.put(amberOperator.id, amberOperatorCopy)
      amberOperatorsCopy.put(amberOperator.id, amberOperator)
    })

    // update the input and output port maps of OpExecConfigs with the link identities
    val outLinks: mutable.Map[OperatorIdentity, mutable.Set[OperatorIdentity]] = mutable.Map()
    logicalPlan.links.foreach(link => {
      val origin = OperatorIdentity(this.context.jobId, link.origin.operatorID)
      val dest = OperatorIdentity(this.context.jobId, link.destination.operatorID)
      outLinks.getOrElseUpdate(origin, mutable.Set()).add(dest)

      val layerLink = LinkIdentity(
        amberOperatorsCopy(origin).topology.layers.last.id,
        amberOperatorsCopy(dest).topology.layers.head.id
      )
      amberOperators(dest).setInputToOrdinalMapping(
        layerLink,
        link.destination.portOrdinal,
        link.destination.portName
      )
      amberOperators(origin)
        .setOutputToOrdinalMapping(layerLink, link.origin.portOrdinal, link.origin.portName)
    })

    // create pipelined regions.
    val pipelinedRegionsDAG =
      new WorkflowPipelinedRegionsBuilder(
        context,
        logicalPlan.operatorMap,
        inputSchemaMap,
        workflowId,
        amberOperators, // may get changed as materialization operators can be added
        outLinks, // may get changed as links to materialization operators can be added
        opResultStorage
      )
        .buildPipelinedRegions()

    val outLinksImmutable: Map[OperatorIdentity, Set[OperatorIdentity]] =
      outLinks.map({ case (operatorId, links) => operatorId -> links.toSet }).toMap

    new Workflow(
      workflowId,
      amberOperators,
      outLinksImmutable,
      pipelinedRegionsDAG
    )
  }

}
