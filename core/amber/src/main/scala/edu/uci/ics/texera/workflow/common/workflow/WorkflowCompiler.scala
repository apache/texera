package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowPipelinedRegionsBuilder
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.service.{ExecutionsMetadataPersistService, WorkflowCacheChecker}
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
      storage: OpResultStorage,
      reuseStorageSet: Set[String] = Set()
  ) = {
    // create a JSON object that holds pointers to the workflow's results in Mongo
    // TODO in the future, will extract this logic from here when we need pointers to the stats storage
    val resultsJSON = objectMapper.createObjectNode()
    val sinksPointers = objectMapper.createArrayNode()
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
        if (reuseStorageSet.contains(storageKey) && storage.contains(storageKey)) {
          sink.setStorage(storage.get(storageKey))
        } else {
          sink.setStorage(
            storage.create(
              context.executionID + "_",
              storageKey,
              logicalPlan.outputSchemaMap(o.operatorIdentifier).head,
              storageType
            )
          )
          // add the sink collection name to the JSON array of sinks
          sinksPointers.add(context.executionID + "_" + storageKey)
        }
      case _ =>
    }
    // update execution entry in MySQL to have pointers to the mongo collections
    resultsJSON.set("results", sinksPointers)
    ExecutionsMetadataPersistService.updateExistingExecutionVolumnPointers(
      context.executionID,
      resultsJSON.toString
    )
  }

  def amberWorkflow(
      workflowId: WorkflowIdentity,
      opResultStorage: OpResultStorage,
      lastCompletedJob: Option[LogicalPlan] = Option.empty
  ): Workflow = {
    // perform rewrite to reuse cache of previous runs
    val cacheReuses = new WorkflowCacheChecker(lastCompletedJob, logicalPlan).getValidCacheReuse()
    val opsToReuseCache = cacheReuses.intersect(logicalPlan.opsToReuseCache.toSet)
    var rewrittenLogicalPlan =
      WorkflowCacheRewriter.transform(logicalPlan, opResultStorage, opsToReuseCache)
    rewrittenLogicalPlan.operatorMap.values.foreach(initOperator)

    // perform rewrite to enforce progressive computation constraints
    rewrittenLogicalPlan = ProgressiveRetractionEnforcer.enforceDelta(rewrittenLogicalPlan, context)

    // assign sink storage to the logical plan after cache rewrite
    // as it will be converted to the actual physical plan
    assignSinkStorage(rewrittenLogicalPlan, opResultStorage, opsToReuseCache)
    // also assign sink storage to the original logical plan, as the original logical plan
    // will be used to be compared to the subsequent runs
    assignSinkStorage(logicalPlan, opResultStorage, opsToReuseCache)

    val physicalPlan0 = rewrittenLogicalPlan.toPhysicalPlan

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
