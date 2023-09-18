package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowPipelinedRegionsBuilder
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage

import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants

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

  private def assignSinkStorage(
      logicalPlan: LogicalPlan,
      storage: OpResultStorage
  ): Unit = {
    // assign storage to texera-managed sinks before generating exec config
    logicalPlan.operators.foreach {
      case o @ (sink: ProgressiveSinkOpDesc) =>
        val storageKey = sink.getUpstreamId.getOrElse(o.operatorID)
        // due to the size limit of single document in mongoDB (16MB)
        // for sinks visualizing HTMLs which could possibly be large in size, we always use the memory storage.
        val storageType = {
          if (sink.getChartType.contains(VisualizationConstants.HTML_VIZ)) OpResultStorage.MEMORY
          else OpResultStorage.defaultStorageMode
        }

        sink.setStorage(
          storage.create(
            context.executionID + "_",
            storageKey,
            logicalPlan.outputSchemaMap(o.operatorIdentifier).head,
            storageType
          )
        )
      case _ =>
    }
  }

  def amberWorkflow(
      workflowId: WorkflowIdentity,
      opResultStorage: OpResultStorage
  ): Workflow = {

    logicalPlan.operatorMap.values.foreach(initOperator)

    assignSinkStorage(logicalPlan, opResultStorage)

    val physicalPlan0 = logicalPlan.toPhysicalPlan

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
