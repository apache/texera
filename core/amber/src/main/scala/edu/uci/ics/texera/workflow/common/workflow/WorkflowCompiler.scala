package edu.uci.ics.texera.workflow.common.workflow

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.model.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.common.model.tuple.Schema
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.error.ErrorUtils.getStackTraceWithAllCauses
import edu.uci.ics.amber.engine.common.Utils.objectMapper
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.amber.engine.common.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.amber.engine.common.workflowruntimestate.WorkflowAggregatedState.FAILED
import edu.uci.ics.amber.engine.common.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants

import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class WorkflowCompilationResult(
    physicalPlan: Option[PhysicalPlan], // if physical plan is none, the compilation is failed
    operatorIdToInputSchemas: Map[OperatorIdentity, List[Option[Schema]]],
    operatorIdToError: Map[OperatorIdentity, WorkflowFatalError]
)

class WorkflowCompiler(
    context: WorkflowContext
) extends LazyLogging {

  /**
    * Compile a workflow to physical plan, along with the schema propagation result and error(if any)
    *
    * @param logicalPlanPojo the pojo parsed from workflow str provided by user
    * @return WorkflowCompilationResult, containing the physical plan, input schemas per op and error per op
    */
  def compile(
      logicalPlanPojo: LogicalPlanPojo
  ): WorkflowCompilationResult = {
    // first compile the pojo to logical plan
    val errorList = new ArrayBuffer[(OperatorIdentity, Throwable)]()
    val opIdToError = mutable.Map[OperatorIdentity, WorkflowFatalError]()

    var logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)
    logicalPlan = SinkInjectionTransformer.transform(
      logicalPlanPojo.opsToViewResult,
      logicalPlan
    )

    logicalPlan.propagateWorkflowSchema(context, Some(errorList))
    // map compilation errors with op id
    if (errorList.nonEmpty) {
      errorList.foreach {
        case (opId, err) =>
          logger.error("error occurred in logical plan compilation", err)
          opIdToError += (opId -> WorkflowFatalError(
            COMPILATION_ERROR,
            Timestamp(Instant.now),
            err.toString,
            getStackTraceWithAllCauses(err),
            opId.id
          ))
      }
    }

    if (opIdToError.nonEmpty) {
      // encounter errors during compile pojo to logical plan,
      //   so directly return None as physical plan, schema map and non-empty error map
      return WorkflowCompilationResult(
        physicalPlan = None,
        operatorIdToInputSchemas = Map.empty,
        operatorIdToError = opIdToError.toMap
      )
    }
    // from logical plan to physical plan
    val physicalPlan = PhysicalPlan(context, logicalPlan)

    // Extract physical input schemas, excluding internal ports
    val physicalInputSchemas = physicalPlan.operators.map { physicalOp =>
      physicalOp.id -> physicalOp.inputPorts.values
        .filterNot(_._1.id.internal)
        .map {
          case (port, _, schema) => port.id -> schema.toOption
        }
    }

    // Group the physical input schemas by their logical operator ID and consolidate the schemas
    val opIdToInputSchemas = physicalInputSchemas
      .groupBy(_._1.logicalOpId)
      .view
      .mapValues(_.flatMap(_._2).toList.sortBy(_._1.id).map(_._2))
      .toMap

    WorkflowCompilationResult(Some(physicalPlan), opIdToInputSchemas, Map.empty)
  }

  /**
    * After separating the compiler as a standalone service, this function needs to be removed.
    */
  @Deprecated
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

    logicalPlan.propagateWorkflowSchema(context, Some(errorList))

    // report compilation errors
    if (errorList.nonEmpty) {
      val executionErrors = errorList.map {
        case (opId, err) =>
          logger.error("error occurred in logical plan compilation", err)
          WorkflowFatalError(
            COMPILATION_ERROR,
            Timestamp(Instant.now),
            err.toString,
            getStackTraceWithAllCauses(err),
            opId.id
          )
      }
      executionStateStore.metadataStore.updateState(metadataStore =>
        updateWorkflowState(FAILED, metadataStore).addFatalErrors(executionErrors.toSeq: _*)
      )
      throw new RuntimeException("workflow failed to compile")
    }
    logicalPlan
  }

  /**
    * After separating the compiler as a standalone service, this function needs to be removed.
    * The sink storage assignment needs to be pushed to the standalone workflow execution service.
    */
  @Deprecated
  def compile(
      logicalPlanPojo: LogicalPlanPojo,
      opResultStorage: OpResultStorage,
      executionStateStore: ExecutionStateStore
  ): Workflow = {
    // generate a LogicalPlan. The logical plan is the injected with all necessary sinks
    val logicalPlan = compileLogicalPlan(logicalPlanPojo, executionStateStore)

    assignSinkStorage(
      logicalPlan,
      context,
      opResultStorage
    )

    // the PhysicalPlan with topology expanded.
    val physicalPlan = PhysicalPlan(context, logicalPlan)

    Workflow(
      context,
      logicalPlan,
      physicalPlan
    )
  }

  /**
    * Once standalone compiler is done, move this function to the execution service, and change the 1st parameter from LogicalPlan to PhysicalPlan
    */
  @Deprecated
  private def assignSinkStorage(
      logicalPlan: LogicalPlan,
      context: WorkflowContext,
      storage: OpResultStorage,
      reuseStorageSet: Set[OperatorIdentity] = Set()
  ): Unit = {
    // create a JSON object that holds pointers to the workflow's results in Mongo
    val resultsJSON = objectMapper.createObjectNode()
    val sinksPointers = objectMapper.createArrayNode()
    // assign storage to texera-managed sinks before generating exec config
    logicalPlan.operators.foreach {
      case o @ (sink: ProgressiveSinkOpDesc) =>
        val storageKey = sink.getUpstreamId.getOrElse(o.operatorIdentifier)
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
              s"${o.getContext.executionId}_",
              storageKey,
              storageType
            )
          )

          sink.getStorage.setSchema(
            logicalPlan.getOperator(storageKey).outputPortToSchemaMapping.values.head
          )
          // add the sink collection name to the JSON array of sinks
          val storageNode = objectMapper.createObjectNode()
          storageNode.put("storageType", storageType)
          storageNode.put("storageKey", s"${o.getContext.executionId}_$storageKey")
          sinksPointers.add(storageNode)
        }
        storage.get(storageKey)

      case _ =>
    }
    // update execution entry in MySQL to have pointers to the mongo collections
    resultsJSON.set("results", sinksPointers)
    ExecutionsMetadataPersistService.tryUpdateExistingExecution(context.executionId) {
      _.setResult(resultsJSON.toString)
    }
  }

}
