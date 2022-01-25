package edu.uci.ics.texera.web.service

import edu.uci.ics.texera.web.service.JobResultService._
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode.{SET_DELTA, SET_SNAPSHOT}
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.sink.storage.SinkStorageReader

/**
  * OperatorResultService manages the materialized result of an operator.
  * It always keeps the latest snapshot of the computation result.
  */
class ProgressiveResultService(
    sink: ProgressiveSinkOpDesc
) {

  // offset that indicates how many records have been sent to the frontend
  var offset = 0

  // derive the web output mode from the sink operator type
  val webOutputMode: WebOutputMode = {
    (sink.getOutputMode, sink.getChartType) match {
      // visualization sinks use its corresponding mode
      case (SET_SNAPSHOT, Some(_)) => SetSnapshotMode()
      case (SET_DELTA, Some(_))    => SetDeltaMode()
      // Non-visualization sinks use pagination mode
      case (_, None) => PaginationMode()
    }
  }

  /**
    * All execution result tuples for this operator to this moment.
    * For SET_SNAPSHOT output mode: result is the latest snapshot
    * FOR SET_DELTA output mode:
    *   - for insert-only delta: effectively the same as latest snapshot
    *   - for insert-retract delta: the union of all delta outputs, not compacted to a snapshot
    */

  /**
    * Produces the WebResultUpdate to send to frontend from a result update from the engine.
    */
  def convertWebResultUpdate(): WebResultUpdate = {
    val storage = sink.getStorage
    val webUpdate = (webOutputMode, sink.getOutputMode) match {
      case (PaginationMode(), SET_SNAPSHOT) =>
        WebPaginationUpdate(PaginationMode(), storage.getCount, List.empty)

      case (SetSnapshotMode(), SET_SNAPSHOT) =>
        webDataFromTuple(webOutputMode, storage.getAll.toList, sink.getChartType)
      case (SetDeltaMode(), SET_DELTA) =>
        val deltaList = storage.getAllAfter(offset).toList
        offset += deltaList.size
        webDataFromTuple(webOutputMode, deltaList, sink.getChartType)

      // currently not supported mode combinations
      // (PaginationMode, SET_DELTA) | (DataSnapshotMode, SET_DELTA) | (DataDeltaMode, SET_SNAPSHOT)
      case _ =>
        throw new RuntimeException(
          "update mode combination not supported: " + (webOutputMode, sink.getOutputMode)
        )
    }
    webUpdate
  }

  def getSnapshot: WebResultUpdate = {
    val storage = sink.getStorage
    webOutputMode match {
      case PaginationMode() =>
        WebPaginationUpdate(PaginationMode(), storage.getCount, List.empty)
      case SetSnapshotMode() | SetDeltaMode() =>
        webDataFromTuple(webOutputMode, storage.getAll.toList, sink.getChartType)
    }
  }

}
