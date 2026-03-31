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

/**
  * Request body for synchronous workflow execution.
  */
case class SyncExecutionRequest(
    executionName: String,
    logicalPlan: LogicalPlanPojo,
    workflowSettings: Option[WorkflowSettings],
    targetOperatorIds: List[String],
    timeoutSeconds: Int, // Execution timeout in seconds
    maxOperatorResultCharLimit: Int, // Max characters for operator results (uses symmetric truncation)
    maxOperatorResultCellCharLimit: Int // Max characters per cell
)

/**
  * Console message in a simplified format - contains type, title, and details.
  */
case class ConsoleMessageInfo(
    msgType: String,
    title: String,
    message: String
)

/**
  * Per-input-port shape info - reports rows and columns for each input port.
  */
case class PortShape(
    portIndex: Int,
    rows: Long
)

/**
  * Per-operator result info - contains everything about one operator.
  */
case class OperatorInfo(
    state: String,
    inputTuples: Long,
    outputTuples: Long,
    inputPortShapes: Option[List[PortShape]], // per-input-port (rows, columns)
    resultMode: String, // "table" or "visualization"
    result: Option[Any], // JSON array (List[ObjectNode])
    totalRowCount: Option[Int],
    displayedRows: Option[Int],
    truncated: Option[Boolean],
    consoleLogs: Option[List[ConsoleMessageInfo]],
    error: Option[String],
    warnings: Option[List[String]]
)

/**
  * Simplified execution result - just success, state, and per-operator info.
  */
case class SyncExecutionResult(
    success: Boolean,
    state: String,
    operators: Map[String, OperatorInfo],
    compilationErrors: Option[Map[String, String]],
    errors: Option[List[String]]
)

/**
  * Sealed trait representing the reason for execution termination.
  */
sealed trait TerminationReason
case class TerminalStateReached(state: ExecutionMetadataStore) extends TerminationReason
case class ConsoleErrorDetected(consoleState: ExecutionConsoleStore) extends TerminationReason
case class TargetResultsReady(statsState: ExecutionStatsStore) extends TerminationReason

/**
  * REST API resource for synchronous (blocking) workflow execution.
  * Uses Observable-based approach to wait for execution completion.
  */
@Path("/execution")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class SyncExecutionResource extends LazyLogging {

  // Maximum caps - always applied for safety
  private val MAX_OPERATOR_RESULT_CHARS = 100000 // 100,000 characters max
  private val MAX_OPERATOR_RESULT_CELL_CHARS = 20000 // 20,000 characters per cell max

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

    // Apply maximum caps for safety
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

      // Shutdown any previous execution's client
      shutdownPreviousExecution(workflowService)

      // Compute sub-DAG if there's exactly 1 target operator (Execute To behavior)
      val effectiveLogicalPlan =
        computeSubDAGIfNeeded(request.logicalPlan, request.targetOperatorIds)

      // TODO: re-enable validation checks once all operators compile cleanly
      // // Always validate Python UDFs for print statements
      // val printErrors = validateNoPrintStatements(effectiveLogicalPlan)
      // if (printErrors.nonEmpty) {
      //   return SyncExecutionResult(
      //     success = false,
      //     state = "ValidationFailed",
      //     operators = Map.empty,
      //     compilationErrors = Some(printErrors),
      //     errors = Some(printErrors.values.toList)
      //   )
      // }

      // // Pre-compile the workflow to catch errors early
      // val compilationErrors = validateWorkflow(workflowId, effectiveLogicalPlan)
      // if (compilationErrors.nonEmpty) {
      //   return SyncExecutionResult(
      //     success = false,
      //     state = "CompilationFailed",
      //     operators = Map.empty,
      //     compilationErrors = Some(compilationErrors),
      //     errors = Some(compilationErrors.values.toList)
      //   )
      // }

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

      // Initialize and start execution
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

      // Check if workflow already completed (for very fast executions)
      // This handles the race condition where execution finishes before Observable subscription
      val currentState = executionService.executionStateStore.metadataStore.getState
      val currentConsoleState = executionService.executionStateStore.consoleStore.getState
      val currentStatsState = executionService.executionStateStore.statsStore.getState

      // Helper to check if all target operators have completed (not just produced output).
      // This ensures upstream operators have finished sending data before we terminate.
      def allTargetsCompleted(stats: ExecutionStatsStore): Boolean = {
        request.targetOperatorIds.nonEmpty && request.targetOperatorIds.forall { opId =>
          stats.operatorInfo.get(opId).exists { metrics =>
            metrics.operatorState == COMPLETED
          }
        }
      }

      val terminationReason: TerminationReason =
        if (isTerminalState(currentState.state)) {
          // Already in terminal state
          TerminalStateReached(currentState)
        } else if (hasConsoleError(currentConsoleState)) {
          // Already has console error
          ConsoleErrorDetected(currentConsoleState)
        } else if (allTargetsCompleted(currentStatsState)) {
          // All target operators already completed
          TargetResultsReady(currentStatsState)
        } else {
          // Create three termination conditions:
          // 1. Terminal state (COMPLETED, FAILED, KILLED, TERMINATED)
          // 2. Console ERROR message (any operator logs an error)
          // 3. All target operators have produced results

          // Observable for terminal state
          val terminalStateObservable: Observable[TerminationReason] =
            executionService.executionStateStore.metadataStore.getStateObservable
              .filter((state: ExecutionMetadataStore) => isTerminalState(state.state))
              .map[TerminationReason](state => TerminalStateReached(state))

          // Observable for console ERROR messages
          val consoleErrorObservable: Observable[TerminationReason] =
            executionService.executionStateStore.consoleStore.getStateObservable
              .filter((consoleState: ExecutionConsoleStore) => hasConsoleError(consoleState))
              .map[TerminationReason](consoleState => ConsoleErrorDetected(consoleState))

          // Observable for all target operators being completed
          val targetResultsObservable: Observable[TerminationReason] =
            executionService.executionStateStore.statsStore.getStateObservable
              .filter((stats: ExecutionStatsStore) => allTargetsCompleted(stats))
              .map[TerminationReason](stats => TargetResultsReady(stats))

          // Race between all conditions - whichever fires first wins
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
              // Timeout - kill the execution
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

      // Handle termination based on reason
      val (finalState, terminatedByConsoleError, terminatedByTargetResults) =
        terminationReason match {
          case TerminalStateReached(state) =>
            (state, false, false)
          case ConsoleErrorDetected(_) =>
            // Console error detected - kill the workflow and get current state
            killExecution(executionService)
            (executionService.executionStateStore.metadataStore.getState, true, false)
          case TargetResultsReady(_) =>
            // All target operators have results - kill the workflow and mark as completed
            // Wait briefly to allow caching of upstream operator results to complete.
            // The caching happens asynchronously in RegionExecutionCoordinator after operators complete,
            // so we need to give it time before shutting down the client.
            // TODO: A better solution would be to make caching synchronous or signal completion
            // from the engine, avoiding this fixed delay.
            Thread.sleep(500)
            killExecution(executionService)
            // Update state to COMPLETED since we got all the results we need
            executionService.executionStateStore.metadataStore.updateState(metadataStore =>
              updateWorkflowState(COMPLETED, metadataStore)
            )
            (executionService.executionStateStore.metadataStore.getState, false, true)
        }

      // Small delay to ensure results are persisted
      Thread.sleep(500)

      // Get in-memory console state for error extraction (may not be persisted to DB yet)
      val inMemoryConsoleState = terminationReason match {
        case ConsoleErrorDetected(consoleState) => Some(consoleState)
        case _                                  => None
      }

      // Collect results
      val executionId = executionService.workflowContext.executionId
      val operatorInfos = collectOperatorInfos(
        executionId,
        executionService,
        request.targetOperatorIds,
        maxOperatorResultCharLimit,
        maxOperatorResultCellCharLimit,
        inMemoryConsoleState
      )

      // Collect fatal errors
      val fatalErrors = finalState.fatalErrors
        .map(err => s"${err.`type`}: ${err.message}")
        .toList

      // Check for console errors in operator results
      val hasOperatorConsoleError = operatorInfos.values.exists(_.error.isDefined)

      // Determine state string based on termination reason
      val stateString =
        if (terminatedByConsoleError) "Failed"
        else if (terminatedByTargetResults) "Completed"
        else stateToString(finalState.state)

      // Success if: completed normally OR all target results ready, with no errors
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

    // Get operator stats from the state store
    val statsState = executionService.executionStateStore.statsStore.getState
    val operatorStats = statsState.operatorInfo

    // Determine which operators to collect - include both target operators and any with console errors
    val baseTargetOps = if (targetOperatorIds.nonEmpty) {
      targetOperatorIds
    } else {
      operatorStats.keys.toList
    }

    // Also include operators from in-memory console state that have errors
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

      // Extract per-input-port shapes from stats
      val inputPortShapes: Option[List[PortShape]] = stats
        .map { s =>
          s.operatorStatistics.inputMetrics.map { pm =>
            PortShape(pm.portId.id, pm.tupleMetrics.count)
          }.toList
        }
        .filter(_.nonEmpty)

      // Get result
      val (resultMode, result, totalRowCount, displayedRows, truncated) =
        collectOperatorResult(
          executionId,
          opId,
          maxOperatorResultCharLimit,
          maxOperatorResultCellCharLimit
        )

      // Get console logs - first try database, then fallback to in-memory state
      val dbConsoleLogs = collectConsoleLogs(executionId, opId)
      val consoleLogs = dbConsoleLogs.orElse {
        // Fallback to in-memory console state if database logs not available
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

      // Check for error in console logs
      // Prefer the longer of title/message to avoid truncation.
      // Python errors store the full text in `message`; Scala errors
      // store the full text in `title` (with stack trace in `message`).
      val errorMsg = consoleLogs.flatMap(
        _.find(_.msgType == "ERROR").map { e =>
          if (e.message.nonEmpty && e.message.length > e.title.length) e.message
          else e.title
        }
      )

      // Extract warnings (PRINT messages with "WARNING: " prefix)
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
    * Collect result for a single operator with symmetric truncation.
    * Uses incremental fetching with character-based limiting:
    * - Collects tuples from the front until half the limit is reached
    * - Keeps a sliding window buffer of recent tuples for the back
    * - Returns JSON array (List[ObjectNode]) - serialization to table/toon format is done by agent-service
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

          // Use iterator to fetch tuples incrementally
          val tupleIterator = document.get()

          // Handle empty result
          if (totalCount == 0 || !tupleIterator.hasNext) {
            return (
              "table",
              Some(List.empty[ObjectNode].asJava),
              Some(0),
              Some(0),
              Some(false)
            )
          }

          // Check for visualization tuple (special case - single tuple with html/json content)
          val firstTuple = tupleIterator.next()
          if (totalCount == 1 && isVisualizationTuple(firstTuple)) {
            val jsonResults =
              ExecutionResultService.convertTuplesToJson(List(firstTuple), isVisualization = true)
            // Inject __is_visualization__ flag so frontend can render HTML instead of table
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

          // Process first tuple — inject original row index for correct display after truncation
          var rowIndex = 0
          val firstJson = ExecutionResultService.convertTuplesToJson(List(firstTuple)).head
          val truncatedFirst = truncateSingleTuple(firstJson, maxOperatorResultCellCharLimit)
          truncatedFirst.put("__row_index__", rowIndex)
          val firstSize = estimateTupleSize(truncatedFirst, mapper)

          // If even one tuple exceeds limit, return truncated version
          if (firstSize >= maxOperatorResultCharLimit) {
            return (
              "table",
              Some(List(truncatedFirst).asJava),
              Some(totalCount),
              Some(1),
              Some(true)
            )
          }

          // Allocate half the budget for front, half for back
          val halfLimit = maxOperatorResultCharLimit / 2
          val truncationNoticeSize = 50 // Approximate size for truncation metadata

          // Collect front tuples
          val frontTuples = mutable.ListBuffer[ObjectNode](truncatedFirst)
          var frontSize = firstSize
          var processedCount = 1

          // Collect front tuples until we reach half the limit
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
              // This tuple would exceed front limit, start collecting for back
              // Put this tuple in the back buffer
              val backBuffer = mutable.ArrayBuffer[(ObjectNode, Int)]()
              backBuffer += ((truncatedTuple, tupleSize))
              var backSize = tupleSize

              // Continue iterating, keeping a sliding window for the back
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

                // Remove from front of buffer if we exceed back limit
                while (backSize > halfLimit - truncationNoticeSize && backBuffer.size > 1) {
                  val (_, removedSize) = backBuffer.remove(0)
                  backSize -= removedSize
                }
              }

              // Now we have front and back tuples
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

          // If we get here, all tuples fit in the front portion
          // Check if there are more tuples
          if (tupleIterator.hasNext) {
            // Still have tuples, need to collect back portion
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

              // Remove from front of buffer if we exceed back limit
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
            // All tuples fit within the limit
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

  /**
    * Truncate cell values in a single tuple that exceed the character limit.
    */
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

  /**
    * Estimate the serialized size of a tuple as JSON.
    */
  private def estimateTupleSize(
      tuple: ObjectNode,
      mapper: ObjectMapper
  ): Int = {
    mapper.writeValueAsString(tuple).length + 1 // +1 for comma in array
  }

  /**
    * Symmetric truncation for individual cell values.
    */
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

  /**
    * Check if any operator has logged an ERROR console message.
    */
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

  /**
    * Validate Python UDF operators for print statements.
    * Returns a map of operator ID -> error message if print statements are found,
    * or an empty map if no print statements are found.
    */
  private def validateNoPrintStatements(logicalPlan: LogicalPlanPojo): Map[String, String] = {
    import org.apache.texera.amber.operator.PythonCodeValidator
    import org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
    import org.apache.texera.amber.operator.udf.python.source.PythonUDFSourceOpDescV2

    val errors = mutable.Map[String, String]()

    for (op <- logicalPlan.operators) {
      op match {
        case pythonUdf: PythonUDFOpDescV2 =>
          try {
            PythonCodeValidator.validateNoPrint(pythonUdf.code)
          } catch {
            case e: RuntimeException =>
              errors(op.operatorIdentifier.id) = e.getMessage
          }
        case pythonSource: PythonUDFSourceOpDescV2 =>
          try {
            PythonCodeValidator.validateNoPrint(pythonSource.code)
          } catch {
            case e: RuntimeException =>
              errors(op.operatorIdentifier.id) = e.getMessage
          }
        case _ => // Not a Python UDF, skip
      }
    }

    errors.toMap
  }

  /**
    * Validate workflow by attempting to compile it.
    * Returns a map of operator ID -> error message if there are compilation errors,
    * or an empty map if compilation succeeds.
    */
  private def validateWorkflow(
      workflowId: Long,
      logicalPlan: LogicalPlanPojo
  ): Map[String, String] = {
    try {
      val tempContext = new WorkflowContext(WorkflowIdentity(workflowId))
      val compiler = new WorkflowCompiler(tempContext)
      compiler.compile(logicalPlan)
      Map.empty // Compilation succeeded
    } catch {
      case e: Exception =>
        // Extract operator ID from error message if possible
        val errorMsg = Option(e.getMessage).getOrElse("Compilation failed")
        // Try to extract operator ID from the error
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
