package edu.uci.ics.texera.web.service

import akka.actor.Cancellable
import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.websocket.event.{
  PaginatedResultEvent,
  TexeraWebSocketEvent,
  WebResultUpdateEvent
}
import edu.uci.ics.texera.web.model.websocket.request.ResultPaginationRequest
import edu.uci.ics.texera.web.service.JobResultService.WebResultUpdate
import edu.uci.ics.texera.web.storage.{JobStateStore, WorkflowStateStore}
import edu.uci.ics.texera.web.workflowresultstate.OperatorResultMetadata
import edu.uci.ics.texera.web.workflowruntimestate.JobMetadataStore
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.RUNNING
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication}
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object JobResultService {

  val defaultPageSize: Int = 10

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
    * Calculates the dirty pages (pages with changed tuples) between two progressive updates,
    * by comparing the "before" snapshot and "after" snapshot tuple-by-tuple.
    * Used by WebPaginationUpdate
    *
    * @return list of indices of modified pages, index starts from 1
    */
  def calculateDirtyPageIndices(
      beforeSnapshot: List[ITuple],
      afterSnapshot: List[ITuple],
      pageSize: Int
  ): List[Int] = {
    var currentIndex = 1
    var currentIndexPageCount = 0
    val dirtyPageIndices = new mutable.HashSet[Int]()
    for ((before, after) <- beforeSnapshot.zipAll(afterSnapshot, null, null)) {
      if (before == null || after == null || !before.equals(after)) {
        dirtyPageIndices.add(currentIndex)
      }
      currentIndexPageCount += 1
      if (currentIndexPageCount == pageSize) {
        currentIndexPageCount = 0
        currentIndex += 1
      }
    }
    dirtyPageIndices.toList
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
  * WorkflowResultService manages the materialized result of all sink operators in one workflow execution.
  *
  * On each result update from the engine, WorkflowResultService
  *  - update the result data for each operator,
  *  - send result update event to the frontend
  */
class JobResultService(
    val opResultStorage: OpResultStorage,
    val workflowStateStore: WorkflowStateStore
) extends SubscriptionManager {

  var progressiveResults: mutable.HashMap[String, ProgressiveResultService] =
    mutable.HashMap[String, ProgressiveResultService]()
  private val resultPullingFrequency =
    AmberUtils.amberConfig.getInt("web-server.workflow-result-pulling-in-seconds")
  private var resultUpdateCancellable: Cancellable = _

  def attachToJob(
      stateStore: JobStateStore,
      workflowInfo: WorkflowInfo,
      client: AmberClient
  ): Unit = {

    if (resultUpdateCancellable != null && !resultUpdateCancellable.isCancelled) {
      resultUpdateCancellable.cancel()
    }

    unsubscribeAll()

    addSubscription(stateStore.jobMetadataStore.getStateObservable.subscribe {
      newState: JobMetadataStore =>
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
        newState.operatorInfo.foreach {
          case (opId, info) =>
            val oldInfo = oldState.operatorInfo.getOrElse(opId, new OperatorResultMetadata())
            // TODO: frontend now receives snapshots instead of deltas, we can optimize this
            // if (oldInfo.tupleCount != info.tupleCount) {
            buf(opId) =
              progressiveResults(opId).convertWebResultUpdate(oldInfo.tupleCount, info.tupleCount)
          //}
        }
        Iterable(WebResultUpdateEvent(buf.toMap))
      })
    )

    // first clear all the results
    progressiveResults.clear()
    workflowStateStore.resultStore.updateState { state =>
      state.withOperatorInfo(Map.empty)
    }

    // If we have cache sources, make dummy sink operators for displaying results on the frontend.
    workflowInfo.toDAG.getSourceOperators.map(source => {
      workflowInfo.toDAG.getOperator(source) match {
        case cacheSourceOpDesc: CacheSourceOpDesc =>
          val dummySink = new ProgressiveSinkOpDesc()
          dummySink.setStorage(opResultStorage.get(cacheSourceOpDesc.targetSinkStorageId))
          progressiveResults += (
            (
              cacheSourceOpDesc.targetSinkStorageId,
              new ProgressiveResultService(dummySink)
            )
          )
        case other => //skip
      }
    })

    // For cached operators and sinks, create result service so that the results can be displayed.
    workflowInfo.toDAG.getSinkOperators.map(sink => {
      workflowInfo.toDAG.getOperator(sink) match {
        case sinkOp: ProgressiveSinkOpDesc =>
          val service = new ProgressiveResultService(sinkOp)
          sinkOp.getCachedUpstreamId match {
            case Some(upstreamId) => progressiveResults += ((upstreamId, service))
            case None             => progressiveResults += ((sink, service))
          }
        case other => // skip other non-texera-managed sinks, if any
      }
    })
  }

  def handleResultPagination(request: ResultPaginationRequest): TexeraWebSocketEvent = {
    // calculate from index (pageIndex starts from 1 instead of 0)
    val from = request.pageSize * (request.pageIndex - 1)
    val opId = request.operatorID
    val paginationIterable =
      if (opResultStorage.contains(opId)) {
        opResultStorage.get(opId).getRange(from, from + request.pageSize)
      } else {
        Iterable.empty
      }
    val mappedResults = paginationIterable
      .map(tuple => tuple.asKeyValuePairJson())
      .toList
    PaginatedResultEvent.apply(request, mappedResults)
  }

  def onResultUpdate(): Unit = {
    workflowStateStore.resultStore.updateState { oldState =>
      oldState.withOperatorInfo(progressiveResults.map {
        case (id, service) =>
          (id, OperatorResultMetadata(service.sink.getStorage.getCount.toInt))
      }.toMap)
    }
  }

}
