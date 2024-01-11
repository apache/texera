package edu.uci.ics.texera.web.service

import akka.actor.Cancellable
import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode.{SET_DELTA, SET_SNAPSHOT}
import edu.uci.ics.texera.web.model.websocket.event.{
  PaginatedResultEvent,
  TexeraWebSocketEvent,
  WebResultUpdateEvent
}
import edu.uci.ics.texera.web.model.websocket.request.ResultPaginationRequest
import edu.uci.ics.texera.web.service.ExecutionResultService.WebResultUpdate
import edu.uci.ics.texera.web.storage.{
  ExecutionStateStore,
  OperatorResultMetadata,
  WorkflowResultStore,
  WorkflowStateStore
}
import edu.uci.ics.texera.web.workflowruntimestate.ExecutionMetadataStore
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.RUNNING
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication}
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.LogicalPlan
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object ExecutionResultService {

  val defaultPageSize: Int = 5

  // convert Tuple from engine's format to JSON format
  def webDataFromTuple(
      mode: WebOutputMode,
      table: List[ITuple],
      chartType: Option[String]
  ): WebDataUpdate = {
    val tableInJson = table.map(t => t.asInstanceOf[Tuple].asKeyValuePairJson())
    WebDataUpdate(mode, tableInJson, chartType)
  }

  /**
    *  convert Tuple from engine's format to JSON format
    */
  private def tuplesToWebData(
      mode: WebOutputMode,
      table: List[ITuple],
      chartType: Option[String]
  ): WebDataUpdate = {
    val tableInJson = table.map(t => t.asInstanceOf[Tuple].asKeyValuePairJson())
    WebDataUpdate(mode, tableInJson, chartType)
  }

  /**
    * For SET_SNAPSHOT output mode: result is the latest snapshot
    * FOR SET_DELTA output mode:
    *   - for insert-only delta: effectively the same as latest snapshot
    *   - for insert-retract delta: the union of all delta outputs, not compacted to a snapshot
    *
    * Produces the WebResultUpdate to send to frontend from a result update from the engine.
    */
  def convertWebResultUpdate(
      sink: ProgressiveSinkOpDesc,
      oldTupleCount: Int,
      newTupleCount: Int
  ): WebResultUpdate = {
    val webOutputMode: WebOutputMode = {
      (sink.getOutputMode, sink.getChartType) match {
        // visualization sinks use its corresponding mode
        case (SET_SNAPSHOT, Some(_)) => SetSnapshotMode()
        case (SET_DELTA, Some(_))    => SetDeltaMode()
        // Non-visualization sinks use pagination mode
        case (_, None) => PaginationMode()
      }
    }

    val storage = sink.getStorage
    val webUpdate = (webOutputMode, sink.getOutputMode) match {
      case (PaginationMode(), SET_SNAPSHOT) =>
        val numTuples = storage.getCount
        val maxPageIndex =
          Math.ceil(numTuples / ExecutionResultService.defaultPageSize.toDouble).toInt
        WebPaginationUpdate(
          PaginationMode(),
          newTupleCount,
          (1 to maxPageIndex).toList
        )
      case (SetSnapshotMode(), SET_SNAPSHOT) =>
        tuplesToWebData(webOutputMode, storage.getAll.toList, sink.getChartType)
      case (SetDeltaMode(), SET_DELTA) =>
        val deltaList = storage.getAllAfter(oldTupleCount).toList
        tuplesToWebData(webOutputMode, deltaList, sink.getChartType)

      // currently not supported mode combinations
      // (PaginationMode, SET_DELTA) | (DataSnapshotMode, SET_DELTA) | (DataDeltaMode, SET_SNAPSHOT)
      case _ =>
        throw new RuntimeException(
          "update mode combination not supported: " + (webOutputMode, sink.getOutputMode)
        )
    }
    webUpdate
  }

  /**
    * Behavior for different web output modes:
    *  - PaginationMode   (used by view result operator)
    *     - send new number of tuples and dirty page index
    *  - SetSnapshotMode  (used by visualization in snapshot mode)
    *     - send entire snapshot result to frontend
    *  - SetDeltaMode     (used by visualization in delta mode)
    *     - send incremental delta result to frontend
    */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  sealed abstract class WebOutputMode extends Product with Serializable

  /**
    * The result update of one operator that will be sent to the frontend.
    * Can be either WebPaginationUpdate (for PaginationMode)
    * or WebDataUpdate (for SetSnapshotMode or SetDeltaMode)
    */
  sealed abstract class WebResultUpdate extends Product with Serializable

  @JsonTypeName("PaginationMode")
  final case class PaginationMode() extends WebOutputMode

  @JsonTypeName("SetSnapshotMode")
  final case class SetSnapshotMode() extends WebOutputMode

  @JsonTypeName("SetDeltaMode")
  final case class SetDeltaMode() extends WebOutputMode

  case class WebPaginationUpdate(
      mode: PaginationMode,
      totalNumTuples: Long,
      dirtyPageIndices: List[Int]
  ) extends WebResultUpdate

  case class WebDataUpdate(mode: WebOutputMode, table: List[ObjectNode], chartType: Option[String])
      extends WebResultUpdate
}

/**
  * ExecutionResultService manages the materialized result of all sink operators in one workflow execution.
  *
  * On each result update from the engine, WorkflowResultService
  *  - update the result data for each operator,
  *  - send result update event to the frontend
  */
class ExecutionResultService(
    val opResultStorage: OpResultStorage,
    val workflowStateStore: WorkflowStateStore
) extends SubscriptionManager
    with LazyLogging {

  var sinkOperators: mutable.HashMap[OperatorIdentity, ProgressiveSinkOpDesc] =
    mutable.HashMap[OperatorIdentity, ProgressiveSinkOpDesc]()
  private val resultPullingFrequency = AmberConfig.executionResultPollingInSecs
  private var resultUpdateCancellable: Cancellable = _

  def attachToExecution(
      stateStore: ExecutionStateStore,
      logicalPlan: LogicalPlan,
      client: AmberClient
  ): Unit = {

    if (resultUpdateCancellable != null && !resultUpdateCancellable.isCancelled) {
      resultUpdateCancellable.cancel()
    }

    unsubscribeAll()

    addSubscription(stateStore.metadataStore.getStateObservable.subscribe {
      newState: ExecutionMetadataStore =>
        {
          if (newState.state == RUNNING) {
            if (resultUpdateCancellable == null || resultUpdateCancellable.isCancelled) {
              resultUpdateCancellable = TexeraWebApplication
                .scheduleRecurringCallThroughActorSystem(
                  2.seconds,
                  resultPullingFrequency.seconds
                ) {
                  onResultUpdate()
                }
            }
          } else {
            if (resultUpdateCancellable != null) resultUpdateCancellable.cancel()
          }
        }
    })

    addSubscription(
      client
        .registerCallback[WorkflowCompleted](_ => {
          logger.info("Workflow execution completed.")
          if (resultUpdateCancellable.cancel() || resultUpdateCancellable.isCancelled) {
            // immediately perform final update
            onResultUpdate()
          }
        })
    )

    addSubscription(
      client.registerCallback[FatalError](_ =>
        if (resultUpdateCancellable != null) {
          resultUpdateCancellable.cancel()
        }
      )
    )

    addSubscription(
      workflowStateStore.resultStore.registerDiffHandler((oldState, newState) => {
        val buf = mutable.HashMap[String, WebResultUpdate]()
        newState.resultInfo.foreach {
          case (opId, info) =>
            val oldInfo = oldState.resultInfo.getOrElse(opId, OperatorResultMetadata())
            buf(opId.id) = ExecutionResultService.convertWebResultUpdate(
              sinkOperators(opId),
              oldInfo.tupleCount,
              info.tupleCount
            )
        }
        Iterable(WebResultUpdateEvent(buf.toMap))
      })
    )

    // first clear all the results
    sinkOperators.clear()
    workflowStateStore.resultStore.updateState { _ =>
      WorkflowResultStore() // empty result store
    }

    // For operators connected to a sink and sinks,
    // create result service so that the results can be displayed.
    logicalPlan.getTerminalOperatorIds.map(sink => {
      logicalPlan.getOperator(sink) match {
        case sinkOp: ProgressiveSinkOpDesc =>
          sinkOperators += ((sinkOp.getUpstreamId.get, sinkOp))
          sinkOperators += ((sink, sinkOp))
        case other => // skip other non-texera-managed sinks, if any
      }
    })
  }

  def handleResultPagination(request: ResultPaginationRequest): TexeraWebSocketEvent = {
    // calculate from index (pageIndex starts from 1 instead of 0)
    val from = request.pageSize * (request.pageIndex - 1)
    val opId = OperatorIdentity(request.operatorID)
    val paginationIterable =
      if (sinkOperators.contains(opId)) {
        sinkOperators(opId).getStorage.getRange(from, from + request.pageSize)
      } else {
        Iterable.empty
      }
    val mappedResults = paginationIterable
      .map(tuple => tuple.asKeyValuePairJson())
      .toList
    PaginatedResultEvent.apply(request, mappedResults)
  }

  private def onResultUpdate(): Unit = {
    workflowStateStore.resultStore.updateState { _ =>
      val newInfo: Map[OperatorIdentity, OperatorResultMetadata] = sinkOperators.map {
        case (id, sink) =>
          val count = sink.getStorage.getCount.toInt
          val mode = sink.getOutputMode
          val changeDetector =
            if (mode == IncrementalOutputMode.SET_SNAPSHOT) {
              UUID.randomUUID.toString
            } else ""
          (id, OperatorResultMetadata(count, changeDetector))
      }.toMap
      WorkflowResultStore(newInfo)
    }
  }

}
