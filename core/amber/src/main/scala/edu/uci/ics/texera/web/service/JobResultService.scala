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
import edu.uci.ics.texera.web.service.WebResultUpdate.{WebResultUpdate, convertWebResultUpdate}
import edu.uci.ics.texera.web.storage.{
  JobStateStore,
  OperatorResultMetadata,
  WorkflowResultStore,
  WorkflowStateStore
}
import edu.uci.ics.texera.web.workflowruntimestate.JobMetadataStore
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.RUNNING
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication}
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode.{SET_DELTA, SET_SNAPSHOT}
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.LogicalPlan
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object JobResultService {

  val defaultPageSize: Int = 5

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

  var sinkOperators: mutable.HashMap[String, ProgressiveSinkOpDesc] =
    mutable.HashMap[String, ProgressiveSinkOpDesc]()
  private val resultPullingFrequency =
    AmberUtils.amberConfig.getInt("web-server.workflow-result-pulling-in-seconds")
  private var resultUpdateCancellable: Cancellable = _

  def attachToJob(
      stateStore: JobStateStore,
      logicalPlan: LogicalPlan,
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
        newState.resultInfo.foreach {
          case (opId, info) =>
            val oldInfo = oldState.resultInfo.getOrElse(opId, OperatorResultMetadata())
            buf(opId) =
              convertWebResultUpdate(sinkOperators(opId), oldInfo.tupleCount, info.tupleCount)
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
    logicalPlan.getTerminalOperators.map(sink => {
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
    val opId = request.operatorID
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
      val newInfo: Map[String, OperatorResultMetadata] = sinkOperators.map {
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
