package edu.uci.ics.texera.web.service

import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode.{SET_DELTA, SET_SNAPSHOT}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc

object WebResultUpdate {

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
        val maxPageIndex = Math.ceil(numTuples / JobResultService.defaultPageSize.toDouble).toInt
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
}
