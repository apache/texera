package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowPipelinedRegionsBuilder
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator

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

    val physicalPlan0 = logicalPlan.toPhysicalPlan(this.context, opResultStorage)

    // create pipelined regions.
    val physicalPlan1 = new WorkflowPipelinedRegionsBuilder(
      workflowId,
      logicalPlan,
      physicalPlan0,
      new MaterializationRewriter(context, opResultStorage)
    ).buildPipelinedRegions()

    // assign link strategies
    val physicalPlan2 = new PartitionEnforcer(physicalPlan1).enforcePartition()

    new Workflow(workflowId, physicalPlan2)
  }

}
