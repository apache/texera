package edu.uci.ics.texera.web.service

import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowResultUpdate
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.SnapshotMulticast
import edu.uci.ics.texera.web.model.websocket.event.WorkflowAvailableResultEvent.OperatorAvailableResult
import edu.uci.ics.texera.web.model.websocket.event.{
  PaginatedResultEvent,
  TexeraWebSocketEvent,
  WebResultUpdateEvent,
  WorkflowAvailableResultEvent
}
import edu.uci.ics.texera.web.model.websocket.request.ResultPaginationRequest
import edu.uci.ics.texera.web.service.JobResultService.{
  PaginationMode,
  WebPaginationUpdate,
  defaultPageSize
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.{WorkflowCompiler, WorkflowInfo}
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc
import javax.websocket.Session
import rx.lang.scala.Observer

import scala.collection.mutable

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
      totalNumTuples: Int,
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
    workflowInfo: WorkflowInfo,
    opResultStorage: OpResultStorage,
    client: AmberClient
) extends SnapshotMulticast[TexeraWebSocketEvent] {
  var operatorResults: mutable.HashMap[String, OperatorResultService] =
    mutable.HashMap[String, OperatorResultService]()
  val updatedSet: mutable.Set[String] = mutable.HashSet[String]()
  var availableResultMap: Map[String, OperatorAvailableResult] = Map.empty

  client
    .getObservable[WorkflowResultUpdate]
    .subscribe((evt: WorkflowResultUpdate) => onResultUpdate(evt))

  workflowInfo.toDAG.getSinkOperators.map(sink => {
    workflowInfo.toDAG.getOperator(sink) match {
      case desc: CacheSinkOpDesc =>
        val upstreamID = workflowInfo.toDAG.getUpstream(sink).head.operatorID
        val service = new OperatorResultService(upstreamID, workflowInfo, opResultStorage)
        service.uuid = desc.uuid
        operatorResults += ((sink, service))
      case _ =>
        val service = new OperatorResultService(sink, workflowInfo, opResultStorage)
        operatorResults += ((sink, service))
    }
  })

  def handleResultPagination(request: ResultPaginationRequest): Unit = {
    var operatorID = request.operatorID
    if (!operatorResults.contains(operatorID)) {
      val downstreamIDs = workflowInfo.toDAG
        .getDownstream(operatorID)
      // Get the first CacheSinkOpDesc, if exists
      downstreamIDs.find(_.isInstanceOf[CacheSinkOpDesc]).foreach { op =>
        operatorID = op.operatorID
      }
    }
    val opResultService = operatorResults(operatorID)
    // calculate from index (pageIndex starts from 1 instead of 0)
    val from = request.pageSize * (request.pageIndex - 1)
    val paginationResults = opResultService.getResult
      .slice(from, from + request.pageSize)
      .map(tuple => tuple.asInstanceOf[Tuple].asKeyValuePairJson())
    send(PaginatedResultEvent.apply(request, paginationResults))
  }

  def onResultUpdate(
      resultUpdate: WorkflowResultUpdate
  ): Unit = {
    val tmpUpdatedSet = updatedSet.clone()
    for (id <- tmpUpdatedSet) {
      if (!operatorResults.contains(id)) {
        updatedSet.remove(id)
      }
    }

    val webUpdateEvent = operatorResults
      .filter(e => resultUpdate.operatorResults.contains(e._1) || !updatedSet.contains(e._1))
      .map(e => {
        if (resultUpdate.operatorResults.contains(e._1)) {
          val e1 = resultUpdate.operatorResults(e._1)
          val opResultService = operatorResults(e._1)
          val webUpdateEvent = opResultService.convertWebResultUpdate(e1)
          if (workflowInfo.toDAG.getOperator(e._1).isInstanceOf[CacheSinkOpDesc]) {
            val upID = opResultService.operatorID
            (upID, webUpdateEvent)
          } else {
            (e._1, webUpdateEvent)
          }
        } else {
          updatedSet += e._1
          val size = operatorResults(e._1).getResult.size
          (e._1, WebPaginationUpdate(PaginationMode(), size, List.empty))
        }
      })
      .toMap

    // update the result snapshot of each operator
    resultUpdate.operatorResults.foreach(e => operatorResults(e._1).updateResult(e._2))

    // return update event
    send(WebResultUpdateEvent(webUpdateEvent))
  }

  def updateResultFromPreviousRun(
      previousResults: mutable.HashMap[String, OperatorResultService],
      cachedOps: mutable.HashMap[String, OperatorDescriptor]
  ): Unit = {
    operatorResults.foreach(e => {
      if (previousResults.contains(e._2.operatorID)) {
        previousResults(e._2.operatorID) = e._2
      }
    })
    previousResults.foreach(e => {
      if (cachedOps.contains(e._2.operatorID) && !operatorResults.contains(e._2.operatorID)) {
        operatorResults += ((e._2.operatorID, e._2))
      }
    })
  }

  def updateAvailableResult(operators: Iterable[OperatorDescriptor]): Unit = {
    val cachedIDs = mutable.HashSet[String]()
    val cachedIDMap = mutable.HashMap[String, String]()
    operatorResults.foreach(e => cachedIDMap += ((e._2.operatorID, e._1)))
    availableResultMap = operators
      .filter(op => cachedIDMap.contains(op.operatorID))
      .map(op => op.operatorID)
      .map(id => {
        (
          id,
          OperatorAvailableResult(
            cachedIDs.contains(id),
            operatorResults(cachedIDMap(id)).webOutputMode
          )
        )
      })
      .toMap
  }

  override def sendSnapshotTo(observer: Observer[TexeraWebSocketEvent]): Unit = {
    observer.onNext(WorkflowAvailableResultEvent(availableResultMap))
    observer.onNext(WebResultUpdateEvent(operatorResults.map(e => (e._1, e._2.getSnapshot)).toMap))
  }

}
