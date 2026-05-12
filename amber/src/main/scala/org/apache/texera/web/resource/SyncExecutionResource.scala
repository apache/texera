/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.web.resource

import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Auth
import org.apache.texera.amber.config.ApplicationConfig
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext, WorkflowSettings}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  ConsoleMessage,
  ConsoleMessageType
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import org.apache.texera.amber.engine.common.executionruntimestate.{
  ExecutionConsoleStore,
  ExecutionMetadataStore,
  ExecutionStatsStore
}
import io.reactivex.rxjava3.core.Observable
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.OPERATOR_EXECUTIONS
import org.apache.texera.web.model.websocket.request.{LogicalPlanPojo, WorkflowExecuteRequest}
import org.apache.texera.workflow.{LogicalLink, WorkflowCompiler}
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
import org.apache.texera.web.service.{ExecutionResultService, WorkflowService}
import org.apache.texera.web.storage.ExecutionStateStore.updateWorkflowState

import java.net.URI
import java.util.concurrent.TimeUnit
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.ObjectMapper

case class SyncExecutionRequest(
    executionName: String,
    logicalPlan: LogicalPlanPojo,
    workflowSettings: Option[WorkflowSettings],
    targetOperatorIds: List[String],
    timeoutSeconds: Int,
    maxOperatorResultCharLimit: Int,
    maxOperatorResultCellCharLimit: Int
)

case class ConsoleMessageInfo(
    msgType: String,
    title: String,
    message: String
)

case class PortShape(
    portIndex: Int,
    rows: Long
)

case class OperatorInfo(
    state: String,
    inputTuples: Long,
    outputTuples: Long,
    inputPortShapes: Option[List[PortShape]],
    resultMode: String, // "table" or "visualization"
    result: Option[Any], // JSON array (List[ObjectNode])
    totalRowCount: Option[Int],
    displayedRows: Option[Int],
    truncated: Option[Boolean],
    consoleLogs: Option[List[ConsoleMessageInfo]],
    error: Option[String],
    warnings: Option[List[String]]
)

case class SyncExecutionResult(
    success: Boolean,
    state: String,
    operators: Map[String, OperatorInfo],
    compilationErrors: Option[Map[String, String]],
    errors: Option[List[String]]
)

sealed trait TerminationReason
case class TerminalStateReached(state: ExecutionMetadataStore) extends TerminationReason
case class ConsoleErrorDetected(consoleState: ExecutionConsoleStore) extends TerminationReason
case class TargetResultsReady(statsState: ExecutionStatsStore) extends TerminationReason

@Path("/execution")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class SyncExecutionResource extends LazyLogging {

  // Hard caps applied regardless of request — guard against runaway payloads.
  private val MAX_OPERATOR_RESULT_CHARS = 100000
  private val MAX_OPERATOR_RESULT_CELL_CHARS = 20000

  @POST
  @Path("/{wid}/{cuid}/run")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def executeWorkflowSync(
      @PathParam("wid") workflowId: Long,
      @PathParam("cuid") computingUnitId: Int,
      request: SyncExecutionRequest,
      @Auth user: SessionUser
  ): SyncExecutionResult = {
    val timeoutSeconds = request.timeoutSeconds

    val maxOperatorResultCharLimit =
      Math.min(request.maxOperatorResultCharLimit, MAX_OPERATOR_RESULT_CHARS)
    val maxOperatorResultCellCharLimit =
      Math.min(request.maxOperatorResultCellCharLimit, MAX_OPERATOR_RESULT_CELL_CHARS)

    logger.info(
      s"Starting sync execution for workflow $workflowId with limits: " +
        s"maxOperatorResultCharLimit=${request.maxOperatorResultCharLimit} (capped to $maxOperatorResultCharLimit), " +
        s"maxOperatorResultCellCharLimit=${request.maxOperatorResultCellCharLimit} (capped to $maxOperatorResultCellCharLimit)"
    )

    try {
      val workflowService = WorkflowService.getOrCreate(
        WorkflowIdentity(workflowId),
        computingUnitId
      )

      shutdownPreviousExecution(workflowService)

      // "Execute To" semantics: when a single target is given, run only its upstream sub-DAG.
      val effectiveLogicalPlan =
        computeSubDAGIfNeeded(request.logicalPlan, request.targetOperatorIds)

      val executeRequest = WorkflowExecuteRequest(
        executionName = request.executionName,
        engineVersion = "1.0",
        logicalPlan = effectiveLogicalPlan,
        replayFromExecution = None,
        workflowSettings = request.workflowSettings
          .getOrElse(
            WorkflowSettings(dataTransferBatchSize = ApplicationConfig.defaultDataTransferBatchSize)
          ),
        emailNotificationEnabled = false,
        computingUnitId = computingUnitId
      )

      workflowService.initExecutionService(
        executeRequest,
        Some(user.getUser),
        new URI(s"sync-execution://$workflowId")
      )

      val executionService = workflowService.executionService.getValue
      if (executionService == null) {
        return SyncExecutionResult(
          success = false,
          state = "Error",
          operators = Map.empty,
          compilationErrors = None,
          errors = Some(List("Failed to initialize execution service"))
        )
      }

      // Snapshot before subscribing — handles the race where a fast execution finishes
      // before the Observable below sees any state change.
      val currentState = executionService.executionStateStore.metadataStore.getState
      val currentConsoleState = executionService.executionStateStore.consoleStore.getState
      val currentStatsState = executionService.executionStateStore.statsStore.getState

      // Multi-region operators (e.g., HashJoin: build region then probe region) report their
      // aggregated logical state as COMPLETED for a brief window after the first region
      // terminates and before the second region's workers are added to regionExecutions.
      // Guard against firing during that window by also requiring every declared external
      // input port to be present in the operator's input metrics — port-1 stats only appear
      // once probe actually starts consuming, which closes the race.
      val targetExpectedExternalInputs: Map[String, Int] = effectiveLogicalPlan.operators
        .filter(op => request.targetOperatorIds.contains(op.operatorIdentifier.id))
        .map(op => op.operatorIdentifier.id -> op.operatorInfo.inputPorts.count(!_.id.internal))
        .toMap

      // Require COMPLETED, not just "has output", so upstream operators finish flushing
      // their data downstream before we tear the execution down.
      def allTargetsCompleted(stats: ExecutionStatsStore): Boolean = {
        request.targetOperatorIds.nonEmpty && request.targetOperatorIds.forall { opId =>
          stats.operatorInfo.get(opId).exists { metrics =>
            val externalInputPortsReporting =
              metrics.operatorStatistics.inputMetrics.count(!_.portId.internal)
            val expectedExternalInputs = targetExpectedExternalInputs.getOrElse(opId, 0)
            metrics.operatorState == COMPLETED &&
            externalInputPortsReporting >= expectedExternalInputs
          }
        }
      }

      val terminationReason: TerminationReason =
        if (isTerminalState(currentState.state)) {
          TerminalStateReached(currentState)
        } else if (hasConsoleError(currentConsoleState)) {
          ConsoleErrorDetected(currentConsoleState)
        } else if (allTargetsCompleted(currentStatsState)) {
          TargetResultsReady(currentStatsState)
        } else {
          val terminalStateObservable: Observable[TerminationReason] =
            executionService.executionStateStore.metadataStore.getStateObservable
              .filter((state: ExecutionMetadataStore) => isTerminalState(state.state))
              .map[TerminationReason](state => TerminalStateReached(state))

          val consoleErrorObservable: Observable[TerminationReason] =
            executionService.executionStateStore.consoleStore.getStateObservable
              .filter((consoleState: ExecutionConsoleStore) => hasConsoleError(consoleState))
              .map[TerminationReason](consoleState => ConsoleErrorDetected(consoleState))

          val targetResultsObservable: Observable[TerminationReason] =
            executionService.executionStateStore.statsStore.getStateObservable
              .filter((stats: ExecutionStatsStore) => allTargetsCompleted(stats))
              .map[TerminationReason](stats => TargetResultsReady(stats))

          try {
            Observable
              .amb(
                java.util.Arrays.asList(
                  terminalStateObservable,
                  consoleErrorObservable,
                  targetResultsObservable
                )
              )
              .firstOrError()
              .timeout(timeoutSeconds.toLong, TimeUnit.SECONDS)
              .blockingGet()
          } catch {
            case _: java.util.concurrent.TimeoutException =>
              killExecution(executionService)
              return SyncExecutionResult(
                success = false,
                state = "Killed",
                operators = Map.empty,
                compilationErrors = None,
                errors = Some(List(s"Timeout after $timeoutSeconds seconds"))
              )
            case e: Exception =>
              logger.error(s"Error waiting for execution: ${e.getMessage}", e)
              return SyncExecutionResult(
                success = false,
                state = "Error",
                operators = Map.empty,
                compilationErrors = None,
                errors = Some(List(e.getMessage))
              )
          }
        }

      val (finalState, terminatedByConsoleError, terminatedByTargetResults) =
        terminationReason match {
          case TerminalStateReached(state) =>
            (state, false, false)
          case ConsoleErrorDetected(_) =>
            killExecution(executionService)
            (executionService.executionStateStore.metadataStore.getState, true, false)
          case TargetResultsReady(_) =>
            // RegionExecutionCoordinator caches upstream results asynchronously after operators
            // complete; sleep gives that caching a chance to finish before we shut down the client.
            // TODO: replace with a synchronous signal from the engine.
            Thread.sleep(500)
            killExecution(executionService)
            // Override to COMPLETED — we have everything we asked for, even though the engine
            // sees this as a kill.
            executionService.executionStateStore.metadataStore.updateState(metadataStore =>
              updateWorkflowState(COMPLETED, metadataStore)
            )
            (executionService.executionStateStore.metadataStore.getState, false, true)
        }

      // Let the result writer flush before we read storage.
      Thread.sleep(500)

      // Console DB writes lag the in-memory store; pass the latter so error extraction
      // can fall back when the row hasn't landed yet.
      val inMemoryConsoleState = terminationReason match {
        case ConsoleErrorDetected(consoleState) => Some(consoleState)
        case _                                  => None
      }

      val executionId = executionService.workflowContext.executionId
      val operatorInfos = collectOperatorInfos(
        executionId,
        executionService,
        request.targetOperatorIds,
        maxOperatorResultCharLimit,
        maxOperatorResultCellCharLimit,
        inMemoryConsoleState
      )

      val fatalErrors = finalState.fatalErrors
        .map(err => s"${err.`type`}: ${err.message}")
        .toList

      val hasOperatorConsoleError = operatorInfos.values.exists(_.error.isDefined)

      val stateString =
        if (terminatedByConsoleError) "Failed"
        else if (terminatedByTargetResults) "Completed"
        else stateToString(finalState.state)

      val isSuccess = (finalState.state == COMPLETED || terminatedByTargetResults) &&
        !hasOperatorConsoleError && !terminatedByConsoleError

      SyncExecutionResult(
        success = isSuccess,
        state = stateString,
        operators = operatorInfos,
        compilationErrors = None,
        errors = if (fatalErrors.nonEmpty) Some(fatalErrors) else None
      )

    } catch {
      case e: Exception =>
        logger.error(s"Sync execution error: ${e.getMessage}", e)
        handleExecutionError(e)
    }
  }

  private def shutdownPreviousExecution(workflowService: WorkflowService): Unit = {
    try {
      val previousEs = workflowService.executionService.getValue
      if (previousEs != null && previousEs.client != null) {
        logger.info(s"Shutting down previous execution client")
        previousEs.client.shutdown()
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Error shutting down previous execution client: ${e.getMessage}")
    }
  }

  private def killExecution(
      executionService: org.apache.texera.web.service.WorkflowExecutionService
  ): Unit = {
    try {
      if (executionService.client != null) {
        executionService.client.shutdown()
      }
      executionService.executionStateStore.statsStore.updateState(stats =>
        stats.withEndTimeStamp(System.currentTimeMillis())
      )
      executionService.executionStateStore.metadataStore.updateState(metadataStore =>
        updateWorkflowState(KILLED, metadataStore)
      )
    } catch {
      case e: Exception =>
        logger.warn(s"Error killing execution: ${e.getMessage}")
    }
  }

  private def collectOperatorInfos(
      executionId: ExecutionIdentity,
      executionService: org.apache.texera.web.service.WorkflowExecutionService,
      targetOperatorIds: List[String],
      maxOperatorResultCharLimit: Int,
      maxOperatorResultCellCharLimit: Int,
      inMemoryConsoleState: Option[ExecutionConsoleStore] = None
  ): Map[String, OperatorInfo] = {
    val operatorInfos = mutable.Map[String, OperatorInfo]()

    val statsState = executionService.executionStateStore.statsStore.getState
    val operatorStats = statsState.operatorInfo

    val baseTargetOps = if (targetOperatorIds.nonEmpty) {
      targetOperatorIds
    } else {
      operatorStats.keys.toList
    }

    // Pull in any operator that logged a console error even if it isn't a target —
    // otherwise the caller can't see why an upstream op failed.
    val consoleErrorOps = inMemoryConsoleState
      .map { consoleState =>
        consoleState.operatorConsole.keys.toList
      }
      .getOrElse(List.empty)

    val targetOps = (baseTargetOps ++ consoleErrorOps).distinct

    for (opId <- targetOps) {
      val stats = operatorStats.get(opId)
      val (state, inputTuples, outputTuples): (String, Long, Long) = stats match {
        case Some(s) =>
          val inputCount = s.operatorStatistics.inputMetrics.map(_.tupleMetrics.count).sum
          val outputCount = s.operatorStatistics.outputMetrics.map(_.tupleMetrics.count).sum
          (stateToString(s.operatorState), inputCount, outputCount)
        case None => ("Unknown", 0L, 0L)
      }

      val inputPortShapes: Option[List[PortShape]] = stats
        .map { s =>
          s.operatorStatistics.inputMetrics.map { pm =>
            PortShape(pm.portId.id, pm.tupleMetrics.count)
          }.toList
        }
        .filter(_.nonEmpty)

      val (resultMode, result, totalRowCount, displayedRows, truncated) =
        collectOperatorResult(
          executionId,
          opId,
          maxOperatorResultCharLimit,
          maxOperatorResultCellCharLimit
        )

      // DB is authoritative once written; fall back to in-memory state for in-flight runs
      // where the console row hasn't been persisted yet.
      val dbConsoleLogs = collectConsoleLogs(executionId, opId)
      val consoleLogs = dbConsoleLogs.orElse {
        inMemoryConsoleState.flatMap { consoleState =>
          consoleState.operatorConsole
            .get(opId)
            .map { opConsole =>
              opConsole.consoleMessages.map { msg =>
                ConsoleMessageInfo(
                  msgType = msg.msgType.name,
                  title = msg.title,
                  message = msg.message
                )
              }.toList
            }
            .filter(_.nonEmpty)
        }
      }

      // Python writes the full error text to `message`; Scala writes it to `title`
      // (with a stack trace in `message`). Pick whichever is longer to avoid losing detail.
      val errorMsg = consoleLogs.flatMap(
        _.find(_.msgType == "ERROR").map { e =>
          if (e.message.nonEmpty && e.message.length > e.title.length) e.message
          else e.title
        }
      )

      // Convention: PRINT messages prefixed with "WARNING: " surface as warnings.
      val warningMsgs = consoleLogs
        .map(_.filter(_.title.startsWith("WARNING: ")).map(_.title))
        .filter(_.nonEmpty)

      operatorInfos(opId) = OperatorInfo(
        state = state,
        inputTuples = inputTuples,
        outputTuples = outputTuples,
        inputPortShapes = inputPortShapes,
        resultMode = resultMode,
        result = result,
        totalRowCount = totalRowCount,
        displayedRows = displayedRows,
        truncated = truncated,
        consoleLogs = consoleLogs,
        error = errorMsg,
        warnings = warningMsgs
      )
    }

    operatorInfos.toMap
  }

  private def handleExecutionError(e: Exception): SyncExecutionResult = {
    val errorMsg = e.getMessage
    val isCompilationError = errorMsg != null && (
      errorMsg.contains("compilation") ||
        errorMsg.contains("Compilation") ||
        errorMsg.contains("operator") ||
        errorMsg.contains("schema")
    )

    if (isCompilationError) {
      SyncExecutionResult(
        success = false,
        state = "CompilationFailed",
        operators = Map.empty,
        compilationErrors = Some(Map("error" -> errorMsg)),
        errors = Some(List(errorMsg))
      )
    } else {
      SyncExecutionResult(
        success = false,
        state = "Error",
        operators = Map.empty,
        compilationErrors = None,
        errors = Some(List(Option(e.getMessage).getOrElse("Unknown error")))
      )
    }
  }

  /**
    * Symmetric truncation: fill half the char budget from the front of the result, keep a
    * sliding-window of the most recent tuples for the back half. Returns a JSON array;
    * serialization to table/toon format happens in agent-service.
    */
  private def collectOperatorResult(
      executionId: ExecutionIdentity,
      opId: String,
      maxOperatorResultCharLimit: Int,
      maxOperatorResultCellCharLimit: Int
  ): (String, Option[Any], Option[Int], Option[Int], Option[Boolean]) = {
    import com.fasterxml.jackson.databind.node.ObjectNode

    try {
      val storageUriOption = WorkflowExecutionsResource.getResultUriByLogicalPortId(
        executionId,
        OperatorIdentity(opId),
        PortIdentity()
      )

      storageUriOption match {
        case Some(storageUri) =>
          val document = DocumentFactory
            .openDocument(storageUri)
            ._1
            .asInstanceOf[VirtualDocument[Tuple]]

          val totalCount = document.getCount.toInt
          val mapper = new ObjectMapper()
          val tupleIterator = document.get()

          if (totalCount == 0 || !tupleIterator.hasNext) {
            return (
              "table",
              Some(List.empty[ObjectNode].asJava),
              Some(0),
              Some(0),
              Some(false)
            )
          }

          // A single tuple with html-content / json-content is a visualization payload —
          // the frontend renders it as an iframe rather than a table.
          val firstTuple = tupleIterator.next()
          if (totalCount == 1 && isVisualizationTuple(firstTuple)) {
            val jsonResults =
              ExecutionResultService.convertTuplesToJson(List(firstTuple), isVisualization = true)
            jsonResults.foreach(
              _.asInstanceOf[ObjectNode].put("__is_visualization__", true)
            )
            return (
              "visualization",
              Some(jsonResults),
              Some(totalCount),
              Some(1),
              Some(false)
            )
          }

          // __row_index__ preserves the original position so the frontend can show
          // "row N" correctly after symmetric truncation drops the middle.
          var rowIndex = 0
          val firstJson = ExecutionResultService.convertTuplesToJson(List(firstTuple)).head
          val truncatedFirst = truncateSingleTuple(firstJson, maxOperatorResultCellCharLimit)
          truncatedFirst.put("__row_index__", rowIndex)
          val firstSize = estimateTupleSize(truncatedFirst, mapper)

          if (firstSize >= maxOperatorResultCharLimit) {
            return (
              "table",
              Some(List(truncatedFirst).asJava),
              Some(totalCount),
              Some(1),
              Some(true)
            )
          }

          val halfLimit = maxOperatorResultCharLimit / 2
          val truncationNoticeSize = 50 // reserved for the "...skipped..." marker

          val frontTuples = mutable.ListBuffer[ObjectNode](truncatedFirst)
          var frontSize = firstSize
          var processedCount = 1

          while (tupleIterator.hasNext && frontSize < halfLimit) {
            val tuple = tupleIterator.next()
            rowIndex += 1
            processedCount += 1
            val jsonTuple = ExecutionResultService.convertTuplesToJson(List(tuple)).head
            val truncatedTuple = truncateSingleTuple(jsonTuple, maxOperatorResultCellCharLimit)
            truncatedTuple.put("__row_index__", rowIndex)
            val tupleSize = estimateTupleSize(truncatedTuple, mapper)

            if (frontSize + tupleSize <= halfLimit) {
              frontTuples += truncatedTuple
              frontSize += tupleSize
            } else {
              // Front is full — switch to a sliding window for the back half.
              val backBuffer = mutable.ArrayBuffer[(ObjectNode, Int)]()
              backBuffer += ((truncatedTuple, tupleSize))
              var backSize = tupleSize

              while (tupleIterator.hasNext) {
                val t = tupleIterator.next()
                rowIndex += 1
                processedCount += 1
                val jt = ExecutionResultService.convertTuplesToJson(List(t)).head
                val tt = truncateSingleTuple(jt, maxOperatorResultCellCharLimit)
                tt.put("__row_index__", rowIndex)
                val ts = estimateTupleSize(tt, mapper)

                backBuffer += ((tt, ts))
                backSize += ts

                while (backSize > halfLimit - truncationNoticeSize && backBuffer.size > 1) {
                  val (_, removedSize) = backBuffer.remove(0)
                  backSize -= removedSize
                }
              }

              val backTuples = backBuffer.map(_._1).toList
              val allTuples = frontTuples.toList ++ backTuples
              val skippedRows = totalCount - allTuples.size

              return (
                "table",
                Some(allTuples.asJava),
                Some(totalCount),
                Some(allTuples.size),
                Some(skippedRows > 0)
              )
            }
          }

          if (tupleIterator.hasNext) {
            val backBuffer = mutable.ArrayBuffer[(ObjectNode, Int)]()
            var backSize = 0

            while (tupleIterator.hasNext) {
              val t = tupleIterator.next()
              rowIndex += 1
              processedCount += 1
              val jt = ExecutionResultService.convertTuplesToJson(List(t)).head
              val tt = truncateSingleTuple(jt, maxOperatorResultCellCharLimit)
              tt.put("__row_index__", rowIndex)
              val ts = estimateTupleSize(tt, mapper)

              backBuffer += ((tt, ts))
              backSize += ts

              while (backSize > halfLimit - truncationNoticeSize && backBuffer.size > 1) {
                val (_, removedSize) = backBuffer.remove(0)
                backSize -= removedSize
              }
            }

            val backTuples = backBuffer.map(_._1).toList
            val allTuples = frontTuples.toList ++ backTuples
            val skippedRows = totalCount - allTuples.size

            (
              "table",
              Some(allTuples.asJava),
              Some(totalCount),
              Some(allTuples.size),
              Some(skippedRows > 0)
            )
          } else {
            (
              "table",
              Some(frontTuples.toList.asJava),
              Some(totalCount),
              Some(frontTuples.size),
              Some(false)
            )
          }

        case None =>
          ("table", None, None, None, None)
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Error collecting result for operator $opId: ${e.getMessage}", e)
        ("table", None, None, None, None)
    }
  }

  private def truncateSingleTuple(
      tuple: ObjectNode,
      maxCellChars: Int
  ): ObjectNode = {
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.databind.node.TextNode

    val mapper = new ObjectMapper()
    val truncatedTuple = mapper.createObjectNode()
    val fieldNames = tuple.fieldNames()

    while (fieldNames.hasNext) {
      val fieldName = fieldNames.next()
      val fieldValue = tuple.get(fieldName)
      if (fieldValue.isTextual) {
        val text = fieldValue.asText()
        if (text.length > maxCellChars) {
          val truncatedText = symmetricTruncateCellValue(text, maxCellChars)
          truncatedTuple.set(fieldName, new TextNode(truncatedText))
        } else {
          truncatedTuple.set(fieldName, fieldValue)
        }
      } else {
        truncatedTuple.set(fieldName, fieldValue)
      }
    }
    truncatedTuple
  }

  private def estimateTupleSize(
      tuple: ObjectNode,
      mapper: ObjectMapper
  ): Int = {
    mapper.writeValueAsString(tuple).length + 1 // +1 for the array separator
  }

  private def symmetricTruncateCellValue(text: String, maxChars: Int): String = {
    if (text.length <= maxChars) {
      text
    } else {
      val notice = "...[truncated]..."
      val availableChars = maxChars - notice.length
      if (availableChars <= 0) {
        text.substring(0, maxChars)
      } else {
        val halfChars = availableChars / 2
        text.substring(0, halfChars) + notice + text.substring(text.length - halfChars)
      }
    }
  }

  private def isVisualizationTuple(tuple: Tuple): Boolean = {
    try {
      val schema = tuple.getSchema
      val fieldNames = schema.getAttributes.map(_.getName)
      fieldNames.exists(name => name == "html-content" || name == "json-content")
    } catch {
      case _: Exception => false
    }
  }

  private def collectConsoleLogs(
      executionId: ExecutionIdentity,
      opId: String
  ): Option[List[ConsoleMessageInfo]] = {
    try {
      val uriOption = getConsoleMessageUri(executionId, OperatorIdentity(opId))

      uriOption.flatMap { uri =>
        val document = DocumentFactory
          .openDocument(uri)
          ._1
          .asInstanceOf[VirtualDocument[Tuple]]

        val messages = document.get().toList.flatMap { tuple =>
          try {
            val protoString = tuple.getField[String](0)
            val msg = ConsoleMessage.fromAscii(protoString)
            Some(
              ConsoleMessageInfo(
                msgType = msg.msgType.name,
                title = msg.title,
                message = msg.message
              )
            )
          } catch {
            case _: Exception => None
          }
        }

        if (messages.nonEmpty) Some(messages) else None
      }
    } catch {
      case _: Exception => None
    }
  }

  private def getConsoleMessageUri(
      eid: ExecutionIdentity,
      opId: OperatorIdentity
  ): Option[URI] = {
    val context = SqlServer.getInstance().createDSLContext()
    Option(
      context
        .select(OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_URI)
        .from(OPERATOR_EXECUTIONS)
        .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
        .and(OPERATOR_EXECUTIONS.OPERATOR_ID.eq(opId.id))
        .fetchOneInto(classOf[String])
    ).filter(uri => uri != null && uri.nonEmpty)
      .map(s => URI.create(s))
  }

  private def isTerminalState(state: WorkflowAggregatedState): Boolean = {
    state match {
      case COMPLETED | FAILED | KILLED | TERMINATED => true
      case _                                        => false
    }
  }

  private def hasConsoleError(consoleState: ExecutionConsoleStore): Boolean = {
    consoleState.operatorConsole.values.exists { opConsole =>
      opConsole.consoleMessages.exists(_.msgType == ConsoleMessageType.ERROR)
    }
  }

  private def stateToString(state: WorkflowAggregatedState): String = {
    state match {
      case UNINITIALIZED => "Uninitialized"
      case READY         => "Ready"
      case RUNNING       => "Running"
      case PAUSING       => "Pausing"
      case PAUSED        => "Paused"
      case RESUMING      => "Resuming"
      case COMPLETED     => "Completed"
      case FAILED        => "Failed"
      case KILLED        => "Killed"
      case TERMINATED    => "Terminated"
      case _             => "Unknown"
    }
  }

  private def computeSubDAGIfNeeded(
      logicalPlan: LogicalPlanPojo,
      targetOperatorIds: List[String]
  ): LogicalPlanPojo = {
    if (targetOperatorIds.length != 1) {
      return logicalPlan
    }

    val targetOpId = targetOperatorIds.head
    val operatorMap: Map[String, LogicalOp] =
      logicalPlan.operators.map(op => op.operatorIdentifier.id -> op).toMap

    if (!operatorMap.contains(targetOpId)) {
      logger.warn(s"Target operator $targetOpId not found in logical plan, using full DAG")
      return logicalPlan
    }

    val incomingLinks: Map[String, List[LogicalLink]] =
      logicalPlan.links.groupBy(_.toOpId.id)

    val visited = mutable.Set[String]()
    val subDagOperators = mutable.ListBuffer[LogicalOp]()
    val subDagLinks = mutable.ListBuffer[LogicalLink]()

    def dfs(currentOpId: String): Unit = {
      if (visited.contains(currentOpId)) return
      visited.add(currentOpId)

      operatorMap.get(currentOpId).foreach { op =>
        subDagOperators += op
        incomingLinks.getOrElse(currentOpId, List.empty).foreach { link =>
          subDagLinks += link
          dfs(link.fromOpId.id)
        }
      }
    }

    dfs(targetOpId)

    LogicalPlanPojo(
      operators = subDagOperators.toList,
      links = subDagLinks.toList,
      opsToViewResult = targetOperatorIds.filter(id => visited.contains(id)),
      opsToReuseResult = logicalPlan.opsToReuseResult.filter(id => visited.contains(id))
    )
  }

  // Returns operator-id -> error message; empty map means compilation succeeded.
  private def validateWorkflow(
      workflowId: Long,
      logicalPlan: LogicalPlanPojo
  ): Map[String, String] = {
    try {
      val tempContext = new WorkflowContext(WorkflowIdentity(workflowId))
      val compiler = new WorkflowCompiler(tempContext)
      compiler.compile(logicalPlan)
      Map.empty
    } catch {
      case e: Exception =>
        val errorMsg = Option(e.getMessage).getOrElse("Compilation failed")
        val operatorIdPattern = """operator[- ]?(\S+)""".r
        val operatorId = operatorIdPattern
          .findFirstMatchIn(errorMsg.toLowerCase)
          .map(_.group(1))
          .getOrElse("workflow")
        Map(operatorId -> errorMsg)
    }
  }

  @GET
  @Path("/health")
  def healthCheck: Map[String, String] = Map("status" -> "ok")
}
