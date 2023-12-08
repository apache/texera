package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowPipelinedRegionsBuilder
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowCompiler(val logicalPlanPojo: LogicalPlanPojo, workflowContext: WorkflowContext, jobStateStore: JobStateStore) {
  def compileLogicalPlan(opResultStorage: OpResultStorage,
                         lastCompletedJob: Option[LogicalPlan] = Option.empty): (LogicalPlan,LogicalPlan) = {
    var logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo, workflowContext)

    logicalPlan = SinkInjectionTransformer.transform(logicalPlanPojo.opsToViewResult, logicalPlan)

    val opsUsedCache = new mutable.HashSet[String]()
    val rewrittenLogicalPlan =
      WorkflowCacheRewriter.transform(logicalPlan, lastCompletedJob, opResultStorage, logicalPlanPojo.opsToReuseResult.toSet, opsUsedCache)

    // assign sink storage to the logical plan after cache rewrite
    // as it will be converted to the actual physical plan
    assignSinkStorage(rewrittenLogicalPlan, opResultStorage, opsUsedCache.toSet)
    // also assign sink storage to the original logical plan, as the original logical plan
    // will be used to be compared to the subsequent runs
    assignSinkStorage(logicalPlan, opResultStorage, opsUsedCache.toSet)
    (logicalPlan, rewrittenLogicalPlan)
  }


  private def assignSinkStorage(
      logicalPlan: LogicalPlan,
      storage: OpResultStorage,
      reuseStorageSet: Set[String] = Set()
  ): Unit = {
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
              o.context.executionID + "_",
              storageKey,
              storageType
            )
          )
          sink.getStorage.setSchema(logicalPlan.outputSchemaMap(o.operatorIdentifier).head)
          // add the sink collection name to the JSON array of sinks
          sinksPointers.add(o.context.executionID + "_" + storageKey)
        }
      case _ =>
    }
    // update execution entry in MySQL to have pointers to the mongo collections
    resultsJSON.set("results", sinksPointers)
    ExecutionsMetadataPersistService.updateExistingExecutionVolumePointers(
      logicalPlan.context.executionID,
      resultsJSON.toString
    )
  }

  def compile(
      workflowId: WorkflowIdentity,
      opResultStorage: OpResultStorage,
      lastCompletedJob: Option[LogicalPlan] = Option.empty
  ): Workflow = {

    val errorList = new ArrayBuffer[(String, Throwable)]()
    // remove previous error state
    jobStateStore.jobMetadataStore.updateState { metadataStore =>
      metadataStore.withFatalErrors(
        metadataStore.fatalErrors.filter(e => e.`type` != COMPILATION_ERROR)
      )
    }



    val (logicalPlan0, logicalPlan) = compileLogicalPlan(opResultStorage, lastCompletedJob)


    val physicalPlan = PhysicalPlan(logicalPlan)



    // create pipelined regions.
    val executionPlan = new WorkflowPipelinedRegionsBuilder(
      workflowId,
      logicalPlan,
      physicalPlan,
      new MaterializationRewriter(logicalPlan.context, opResultStorage)
    ).buildPipelinedRegions()

    // assign link strategies
    val partitioningPlan = new PartitionEnforcer(physicalPlan, executionPlan).enforcePartition()

    // assert all source layers to have 0 input ports
    physicalPlan.getSourceOperators.foreach { sourceLayer =>
      assert(physicalPlan.getLayer(sourceLayer).inputPorts.isEmpty)
    }

    new Workflow(workflowId, logicalPlan0, logicalPlan, physicalPlan, executionPlan, partitioningPlan)


//        // report compilation errors
//        if (errorList.nonEmpty) {
//          val jobErrors = errorList.map {
//            case (opId, err) =>
////              logger.error("error occurred in logical plan compilation", err)
//              WorkflowFatalError(
//                COMPILATION_ERROR,
//                Timestamp(Instant.now),
//                err.toString,
//                err.getStackTrace.mkString("\n"),
//                opId
//              )
//          }
//          jobStateStore.jobMetadataStore.updateState(metadataStore =>
//            updateWorkflowState(FAILED, metadataStore).addFatalErrors(jobErrors: _*)
//          )
//        }
  }

}
