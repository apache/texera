package edu.uci.ics.texera.workflow.common.workflow

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowPipelinedRegionsBuilder
import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.storage.JobStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.FAILED
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

class WorkflowCompiler(
    val logicalPlanPojo: LogicalPlanPojo,
    workflowContext: WorkflowContext
) extends LazyLogging {

  def compileLogicalPlan(jobStateStore: JobStateStore): LogicalPlan = {

    val errorList = new ArrayBuffer[(OperatorIdentity, Throwable)]()
    // remove previous error state
    jobStateStore.jobMetadataStore.updateState { metadataStore =>
      metadataStore.withFatalErrors(
        metadataStore.fatalErrors.filter(e => e.`type` != COMPILATION_ERROR)
      )
    }

    var logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo, workflowContext)
    logicalPlan = SinkInjectionTransformer.transform(
      logicalPlanPojo.opsToViewResult,
      logicalPlan
    )

    logicalPlan = logicalPlan.propagateWorkflowSchema(Some(errorList))

    // report compilation errors
    if (errorList.nonEmpty) {
      val jobErrors = errorList.map {
        case (opId, err) =>
          logger.error("error occurred in logical plan compilation", err)
          WorkflowFatalError(
            COMPILATION_ERROR,
            Timestamp(Instant.now),
            err.toString,
            err.getStackTrace.mkString("\n"),
            opId.id
          )
      }
      jobStateStore.jobMetadataStore.updateState(metadataStore =>
        updateWorkflowState(FAILED, metadataStore).addFatalErrors(jobErrors: _*)
      )
    }
    logicalPlan
  }

  def compile(
      workflowId: WorkflowIdentity,
      opResultStorage: OpResultStorage,
      lastCompletedJob: Option[LogicalPlan] = Option.empty,
      jobStateStore: JobStateStore
  ): Workflow = {

    // generate an original LogicalPlan. The logical plan is the injected with all necessary sinks
    //  this plan will be compared in subsequent runs to check which operator can be replaced
    //  by cache.
    val originalLogicalPlan = compileLogicalPlan(jobStateStore)

    // the cache-rewritten LogicalPlan. It is considered to be equivalent with the original plan.
    val rewrittenLogicalPlan = WorkflowCacheRewriter.transform(
      originalLogicalPlan,
      lastCompletedJob,
      opResultStorage,
      logicalPlanPojo.opsToReuseResult.map(idString => OperatorIdentity(idString)).toSet
    )

    // the PhysicalPlan with topology expanded.
    var physicalPlan = PhysicalPlan(workflowId.executionId, rewrittenLogicalPlan)

    // generate an ExecutionPlan with regions.
    //  currently, WorkflowPipelinedRegionsBuilder is the only ExecutionPlan generator.
    val pipelinedRegionsBuilder = new WorkflowPipelinedRegionsBuilder(
      workflowId,
      rewrittenLogicalPlan,
      physicalPlan,
      new MaterializationRewriter(rewrittenLogicalPlan.context, opResultStorage)
    )
    val executionPlan = pipelinedRegionsBuilder.buildPipelinedRegions()

    // get the updated physical plan
    physicalPlan = pipelinedRegionsBuilder.physicalPlan

    // assert all source layers to have 0 input ports
    physicalPlan.getSourceOperatorIds.foreach { sourcePhysicalOpId =>
      assert(physicalPlan.getOperator(sourcePhysicalOpId).inputPorts.isEmpty)
    }
    // assert all sink layers to have 0 output ports
    physicalPlan.getSinkOperatorIds.foreach { sinkPhysicalOpId =>
      assert(physicalPlan.getOperator(sinkPhysicalOpId).outputPorts.isEmpty)
    }

    new Workflow(
      workflowId,
      originalLogicalPlan,
      rewrittenLogicalPlan,
      physicalPlan,
      executionPlan
    )

  }

}
