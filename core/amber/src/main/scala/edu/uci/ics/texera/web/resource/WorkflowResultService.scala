package edu.uci.ics.texera.web.resource

import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowResultUpdate
import edu.uci.ics.amber.engine.architecture.principal.OperatorResult
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.resource.WorkflowResultService.{
  PaginationMode,
  SetDeltaMode,
  SetSnapshotMode,
  WebOutputMode,
  WebPaginationUpdate,
  WebResultUpdate,
  defaultPageSize,
  webDataFromTuple
}
import edu.uci.ics.texera.web.resource.WorkflowResultService.calculateDirtyPageIndices
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource.send
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode.{SET_DELTA, SET_SNAPSHOT}
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import javax.websocket.Session
import scala.collection.mutable

object WorkflowResultService {

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
    *            or WebDataUpdate (for SetSnapshotMode or SetDeltaMode)
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
  * WorkflowResultService manages the materialized result of all sink operators in one workflow execution.
  *
  * On each result update from the engine, WorkflowResultService
  *  - update the result data for each operator,
  *  - send result update event to the frontend
  */
class WorkflowResultService(val workflowCompiler: WorkflowCompiler) {

  // OperatorResultService for each sink operator
  val operatorResults: Map[String, OperatorResultService] =
    workflowCompiler.workflow.getSinkOperators
      .map(sink => (sink, new OperatorResultService(sink, workflowCompiler)))
      .toMap

  def onResultUpdate(resultUpdate: WorkflowResultUpdate, session: Session): Unit = {

    // prepare web update event to frontend
    val webUpdateEvent = resultUpdate.operatorResults.map(e => {
      val opResultService = operatorResults(e._1)
      val webUpdateEvent = opResultService.convertWebResultUpdate(e._2)
      (e._1, webUpdateEvent)
    })

    // update the result snapshot of each operator
    resultUpdate.operatorResults.foreach(e => operatorResults(e._1).updateResult(e._2))

    // send update event to frontend
    send(session, WebResultUpdateEvent(webUpdateEvent))

  }

  /**
    * OperatorResultService manages the materialized result of an operator.
    * It always keeps the latest snapshot of the computation result.
    */
  class OperatorResultService(val operatorID: String, val workflowCompiler: WorkflowCompiler) {

    // derive the web output mode from the sink operator type
    val webOutputMode: WebOutputMode = {
      val op = workflowCompiler.workflow.getOperator(operatorID)
      if (!op.isInstanceOf[SimpleSinkOpDesc]) {
        throw new RuntimeException("operator is not sink: " + op.operatorID)
      }
      val sink = op.asInstanceOf[SimpleSinkOpDesc]
      (sink.getOutputMode, sink.getChartType) match {
        // visualization sinks use its corresponding mode
        case (SET_SNAPSHOT, Some(_)) => SetSnapshotMode()
        case (SET_DELTA, Some(_))    => SetDeltaMode()
        // Non-visualization sinks use pagination mode
        case (_, None) => PaginationMode()
      }
    }

    // chartType of this sink operator
    val chartType: Option[String] = {
      val op = workflowCompiler.workflow.getOperator(operatorID)
      if (!op.isInstanceOf[SimpleSinkOpDesc]) {
        throw new RuntimeException("operator is not sink: " + op.operatorID)
      }
      op.asInstanceOf[SimpleSinkOpDesc].getChartType
    }

    /**
      * All execution result tuples for this operator to this moment.
      * For SET_SNAPSHOT output mode: result is the latest snapshot
      * FOR SET_DELTA output mode:
      *   - for insert-only delta: effectively the same as latest snapshot
      *   - for insert-retract delta: the union of all delta outputs, not compacted to a snapshot
      */
    private var result: List[ITuple] = List()

    /**
      * Produces the WebResultUpdate to send to frontend from a result update from the engine.
      */
    def convertWebResultUpdate(resultUpdate: OperatorResult): WebResultUpdate = {
      (webOutputMode, resultUpdate.outputMode) match {
        case (PaginationMode(), SET_SNAPSHOT) =>
          val dirtyPageIndices =
            calculateDirtyPageIndices(result, resultUpdate.result, defaultPageSize)
          WebPaginationUpdate(PaginationMode(), resultUpdate.result.size, dirtyPageIndices)

        case (SetSnapshotMode(), SET_SNAPSHOT) | (SetDeltaMode(), SET_DELTA) =>
          webDataFromTuple(webOutputMode, resultUpdate.result, chartType)

        // currently not supported mode combinations
        // (PaginationMode, SET_DELTA) | (DataSnapshotMode, SET_DELTA) | (DataDeltaMode, SET_SNAPSHOT)
        case _ =>
          throw new RuntimeException(
            "update mode combination not supported: " + (webOutputMode, resultUpdate.outputMode)
          )
      }
    }

    /**
      * Updates the current result of this operator.
      */
    def updateResult(resultUpdate: OperatorResult): Unit = {
      resultUpdate.outputMode match {
        case SET_SNAPSHOT =>
          this.result = resultUpdate.result
        case SET_DELTA =>
          this.result = (this.result ++ resultUpdate.result)
      }
    }

    def getResult: List[ITuple] = this.result

  }
}
