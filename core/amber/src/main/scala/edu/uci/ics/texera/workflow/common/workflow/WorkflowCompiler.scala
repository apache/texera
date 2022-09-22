package edu.uci.ics.texera.workflow.common.workflow

import com.google.common.base.Verify
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
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
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
  logicalPlan.operators.values.foreach(initOperator)

  def initOperator(operator: OperatorDescriptor): Unit = {
    operator.setContext(context)
  }

  def validate: Map[String, Set[ConstraintViolation]] =
    this.logicalPlan.operators
      .map(o => (o._1, o._2.validate().toSet))
      .filter(o => o._2.nonEmpty)

  def amberWorkflow(workflowId: WorkflowIdentity, opResultStorage: OpResultStorage): Workflow = {
    // TODO: change pre-process to also be immutable and create a copy of each opDesc
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

    val physicalPlan = logicalPlan.toPhysicalPlan(this.context, opResultStorage)

    // create pipelined regions.
    val newPhysicalPlan = new WorkflowPipelinedRegionsBuilder(
      workflowId,
      logicalPlan,
      physicalPlan,
      new MaterializationRewriter(context, opResultStorage)
    )
      .buildPipelinedRegions()

    new Workflow(workflowId, newPhysicalPlan)
  }

}
