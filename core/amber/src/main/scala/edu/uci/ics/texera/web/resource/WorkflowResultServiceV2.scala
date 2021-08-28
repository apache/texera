package edu.uci.ics.texera.web.resource

import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowResultUpdate
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.resource.WorkflowResultServiceV2.{
  PaginationMode,
  WebPaginationUpdate,
  WebResultUpdate,
  defaultPageSize
}
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource.send
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDescV2

import javax.websocket.Session
import scala.collection.mutable

object WorkflowResultServiceV2 {

  val defaultPageSize: Int = 10

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

  @JsonTypeName("PaginationMode")
  final case class PaginationMode() extends WebOutputMode

  @JsonTypeName("SetSnapshotMode")
  final case class SetSnapshotMode() extends WebOutputMode

  @JsonTypeName("SetDeltaMode")
  final case class SetDeltaMode() extends WebOutputMode

  /**
    * The result update of one operator that will be sent to the frontend.
    * Can be either WebPaginationUpdate (for PaginationMode)
    * or WebDataUpdate (for SetSnapshotMode or SetDeltaMode)
    */
  sealed abstract class WebResultUpdate extends Product with Serializable

  case class WebPaginationUpdate(
      mode: PaginationMode,
      totalNumTuples: Int,
      dirtyPageIndices: List[Int]
  ) extends WebResultUpdate

  case class WebDataUpdate(mode: WebOutputMode, table: List[ObjectNode], chartType: Option[String])
      extends WebResultUpdate

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
}

case class WebResultUpdateEvent(updates: Map[String, WebResultUpdate]) extends TexeraWebSocketEvent

/**
  * WorkflowResultServiceV2 manages the materialized result of all sink operators in one workflow execution.
  *
  * On each result update from the engine, WorkflowResultServiceV2
  *  - update the result data for each operator,
  *  - send result update event to the frontend
  */
class WorkflowResultServiceV2(
    val workflowCompiler: WorkflowCompiler,
    opResultStorage: OpResultStorage
) {

  // OperatorResultService for each sink operator
  var operatorResults: mutable.HashMap[String, OperatorResultServiceV2] =
    mutable.HashMap[String, OperatorResultServiceV2]()
  workflowCompiler.workflow.getSinkOperators.map(sink => {
    if (workflowCompiler.workflow.getOperator(sink).isInstanceOf[CacheSinkOpDescV2]) {
      val upstreamID = workflowCompiler.workflow.getUpstream(sink).head.operatorID
      val serviceV2 = new OperatorResultServiceV2(upstreamID, workflowCompiler, opResultStorage)
      operatorResults += ((sink, serviceV2))
    } else {
      val serviceV2 = new OperatorResultServiceV2(sink, workflowCompiler, opResultStorage)
      operatorResults += ((sink, serviceV2))
    }
  })

  def onResultUpdate(resultUpdate: WorkflowResultUpdate, session: Session): Unit = {

    val webUpdateEvent = operatorResults
      .map(e => {
        if (resultUpdate.operatorResults.contains(e._1)) {
          val e1 = resultUpdate.operatorResults(e._1)
          val opResultService = operatorResults(e._1)
          val webUpdateEvent = opResultService.convertWebResultUpdate(e1)
          if (workflowCompiler.workflow.getOperator(e._1).isInstanceOf[CacheSinkOpDescV2]) {
            val upID = opResultService.operatorID
            (upID, webUpdateEvent)
          } else {
            (e._1, webUpdateEvent)
          }
        } else {
          val size = operatorResults(e._1).getResult.size
          (e._1, WebPaginationUpdate(PaginationMode(), size, List.range(0, defaultPageSize)))
        }
      })
      .toMap

    // update the result snapshot of each operator
    resultUpdate.operatorResults.foreach(e => operatorResults(e._1).updateResult(e._2))

    // send update event to frontend
    send(session, WebResultUpdateEvent(webUpdateEvent))

  }

}
