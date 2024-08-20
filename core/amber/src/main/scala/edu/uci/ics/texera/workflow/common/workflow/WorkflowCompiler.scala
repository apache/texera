package edu.uci.ics.texera.workflow.common.workflow

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.error.ErrorUtils.getStackTraceWithAllCauses
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.FAILED
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants

import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class WorkflowCompilationResult(
    physicalPlan: PhysicalPlan,
    operatorIdToInputSchemas: Map[OperatorIdentity, List[Option[Schema]]],
    operatorIdToError: Map[OperatorIdentity, WorkflowFatalError]
)

class WorkflowCompiler(
    context: WorkflowContext
) extends LazyLogging {

  def compileToLogicalPlan(
      logicalPlanPojo: LogicalPlanPojo
  ): (LogicalPlan, Map[OperatorIdentity, WorkflowFatalError]) = {
    val errorList = new ArrayBuffer[(OperatorIdentity, Throwable)]()
    val opIdToError = mutable.Map[OperatorIdentity, WorkflowFatalError]()

    var logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)
    logicalPlan = SinkInjectionTransformer.transform(
      logicalPlanPojo.opsToViewResult,
      logicalPlan
    )

    logicalPlan.propagateWorkflowSchema(context, Some(errorList))
    // report compilation errors
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
    (logicalPlan, opIdToError.toMap)
  }
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

  def compile(
      logicalPlanPojo: LogicalPlanPojo,
      opResultStorage: OpResultStorage,
      executionStateStore: ExecutionStateStore
  ): Workflow = {
    // generate a LogicalPlan. The logical plan is the injected with all necessary sinks
    val logicalPlan = compileLogicalPlan(logicalPlanPojo, executionStateStore)

    // assign the storage location to sink operators
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

  def cleanCompile(
      logicalPlanPojo: LogicalPlanPojo
  ): WorkflowCompilationResult = {
    val (logicalPlan, opIdToError) = compileToLogicalPlan(logicalPlanPojo)
    if (opIdToError.nonEmpty) {
      // encounter error during compile the logical plan pojo to logical plan,
      // so directly return empty physical plan, empty schema map and error
      return WorkflowCompilationResult(
        physicalPlan = PhysicalPlan.empty,
        operatorIdToInputSchemas = Map.empty,
        operatorIdToError = opIdToError
      )
    }
    // the PhysicalPlan with topology expanded.
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

    WorkflowCompilationResult(physicalPlan, opIdToInputSchemas, Map.empty)
  }

  private def assignSinkStorage(
      logicalPlan: LogicalPlan,
      context: WorkflowContext,
      storage: OpResultStorage,
      reuseStorageSet: Set[OperatorIdentity] = Set()
  ): Unit = {
    // create a JSON object that holds pointers to the workflow's results in Mongo
    // TODO in the future, will extract this logic from here when we need pointers to the stats storage
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
