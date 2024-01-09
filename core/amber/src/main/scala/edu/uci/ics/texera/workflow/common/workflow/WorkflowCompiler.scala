package edu.uci.ics.texera.workflow.common.workflow

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.architecture.scheduling.ExpansionGreedyRegionPlanGenerator
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.FAILED
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

class WorkflowCompiler(
    context: WorkflowContext
) extends LazyLogging {

  def compileLogicalPlan(
      logicalPlanPojo: LogicalPlanPojo,
      executionStateStore: ExecutionStateStore
  ): LogicalPlan = {

    val errorList = new ArrayBuffer[(OperatorIdentity, Throwable)]()
    // remove previous error state
    executionStateStore.metadataStore.updateState { metadataStore =>
      metadataStore.withFatalErrors(
        metadataStore.fatalErrors.filter(e => e.`type` != COMPILATION_ERROR)
      )
    }

    var logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)
    logicalPlan = SinkInjectionTransformer.transform(
      logicalPlanPojo.opsToViewResult,
      logicalPlan
    )

    logicalPlan = logicalPlan.propagateWorkflowSchema(context, Some(errorList))

    // report compilation errors
    if (errorList.nonEmpty) {
      val executionErrors = errorList.map {
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
      executionStateStore.metadataStore.updateState(metadataStore =>
        updateWorkflowState(FAILED, metadataStore).addFatalErrors(executionErrors: _*)
      )
    }
    logicalPlan
  }

  def compile(
      logicalPlanPojo: LogicalPlanPojo,
      opResultStorage: OpResultStorage,
      lastCompletedExecutionLogicalPlan: Option[LogicalPlan] = Option.empty,
      executionStateStore: ExecutionStateStore,
      controllerConfig: ControllerConfig
  ): Workflow = {

    // generate an original LogicalPlan. The logical plan is the injected with all necessary sinks
    //  this plan will be compared in subsequent runs to check which operator can be replaced
    //  by cache.
    val originalLogicalPlan = compileLogicalPlan(logicalPlanPojo, executionStateStore)

    // the cache-rewritten LogicalPlan. It is considered to be equivalent with the original plan.
    val rewrittenLogicalPlan = WorkflowCacheRewriter.transform(
      context,
      originalLogicalPlan,
      lastCompletedExecutionLogicalPlan,
      opResultStorage,
      logicalPlanPojo.opsToReuseResult.map(idString => OperatorIdentity(idString)).toSet
    )

    // the PhysicalPlan with topology expanded.
    val physicalPlan = PhysicalPlan(context, rewrittenLogicalPlan)

    // generate an RegionPlan with regions.
    //  currently, ExpansionGreedyRegionPlanGenerator is the only RegionPlan generator.
    val (regionPlan, updatedPhysicalPlan) = new ExpansionGreedyRegionPlanGenerator(
      rewrittenLogicalPlan,
      physicalPlan,
      opResultStorage,
      controllerConfig
    ).generate(context)

    // validate the plan
    // TODO: generalize validation to each plan
    // the updated physical plan's all source operators should have 0 input ports
    updatedPhysicalPlan.getSourceOperatorIds.foreach { sourcePhysicalOpId =>
      assert(updatedPhysicalPlan.getOperator(sourcePhysicalOpId).inputPorts.isEmpty)
    }
    // the updated physical plan's all sink operators should have 0 output ports
    updatedPhysicalPlan.getSinkOperatorIds.foreach { sinkPhysicalOpId =>
      assert(updatedPhysicalPlan.getOperator(sinkPhysicalOpId).outputPorts.isEmpty)
    }

    Workflow(
      context,
      originalLogicalPlan,
      rewrittenLogicalPlan,
      updatedPhysicalPlan,
      regionPlan
    )

  }

}
