package edu.uci.ics.texera.web.service

import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowResultUpdate
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.websocket.event.WebResultUpdateEvent
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource.send
import edu.uci.ics.texera.web.service.WorkflowResultService.{
  PaginationMode,
  WebPaginationUpdate,
  defaultPageSize
}
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc

import javax.websocket.Session
import scala.collection.mutable

object WorkflowResultService {

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
class WorkflowResultService(
    val workflowCompiler: WorkflowCompiler,
    opResultStorage: OpResultStorage
) {
  val updatedSet: mutable.Set[String] = mutable.HashSet[String]()

  // OperatorResultService for each sink operator
  var operatorResults: mutable.HashMap[String, OperatorResultService] =
    mutable.HashMap[String, OperatorResultService]()

  workflowCompiler.workflow.getSinkOperators.map(sink => {
    workflowCompiler.workflow.getOperator(sink) match {
      case desc: CacheSinkOpDesc =>
        val upstreamID = workflowCompiler.workflow.getUpstream(sink).head.operatorID
        val service = new OperatorResultService(upstreamID, workflowCompiler, opResultStorage)
        service.uuid = desc.uuid
        operatorResults += ((sink, service))
      case _ =>
        val service = new OperatorResultService(sink, workflowCompiler, opResultStorage)
        operatorResults += ((sink, service))
    }
  })

  def onResultUpdate(resultUpdate: WorkflowResultUpdate, session: Session): Unit = {

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
          if (workflowCompiler.workflow.getOperator(e._1).isInstanceOf[CacheSinkOpDesc]) {
            val upID = opResultService.operatorID
            (upID, webUpdateEvent)
          } else {
            (e._1, webUpdateEvent)
          }
        } else {
          updatedSet += e._1
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
