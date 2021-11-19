package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.engine.architecture.principal.OperatorResult
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.service.JobResultService._
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode.{SET_DELTA, SET_SNAPSHOT}
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo
import edu.uci.ics.texera.workflow.operators.sink.{CacheSinkOpDesc, SimpleSinkOpDesc}

/**
  * OperatorResultService manages the materialized result of an operator.
  * It always keeps the latest snapshot of the computation result.
  */
class OperatorResultService(
    val operatorID: String,
    val workflowInfo: WorkflowInfo,
    opResultStorage: OpResultStorage
) {

  var uuid: String = _

  assert(workflowInfo.cachedOperatorIds != null)

  // derive the web output mode from the sink operator type
  val webOutputMode: WebOutputMode = {
    val op = workflowInfo.toDAG.getOperator(operatorID)
    if (!op.isInstanceOf[SimpleSinkOpDesc]) {
      PaginationMode()
      //        throw new RuntimeException("operator is not sink: " + op.operatorID)
    } else {
      val sink = op.asInstanceOf[SimpleSinkOpDesc]
      (sink.getOutputMode, sink.getChartType) match {
        // visualization sinks use its corresponding mode
        case (SET_SNAPSHOT, Some(_)) => SetSnapshotMode()
        case (SET_DELTA, Some(_))    => SetDeltaMode()
        // Non-visualization sinks use pagination mode
        case (_, None) => PaginationMode()
      }
    }

  }

  // chartType of this sink operator
  val chartType: Option[String] = {
    val op = workflowInfo.toDAG.getOperator(operatorID)
    if (!op.isInstanceOf[SimpleSinkOpDesc]) {
      new SimpleSinkOpDesc().getChartType
      //        throw new RuntimeException("operator is not sink: " + op.operatorID)
    } else {
      op.asInstanceOf[SimpleSinkOpDesc].getChartType
    }
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
    workflowInfo.toDAG.getOperator(operatorID) match {
      case op: CacheSinkOpDesc =>
        resultUpdate.outputMode match {
          case SET_SNAPSHOT =>
            opResultStorage.put(op.uuid, resultUpdate.result.asInstanceOf[List[Tuple]])
          case SET_DELTA =>
            val tmp = opResultStorage.get(op.uuid)
            opResultStorage.put(op.uuid, (tmp ++ resultUpdate.result.asInstanceOf[List[Tuple]]))
        }
      case _ =>
        resultUpdate.outputMode match {
          case SET_SNAPSHOT =>
            this.result = resultUpdate.result
          case SET_DELTA =>
            this.result = (this.result ++ resultUpdate.result)
        }
    }
  }

  def getResult: List[ITuple] = {
    if (workflowInfo.cachedOperatorIds.contains(operatorID)) {
      opResultStorage.get(uuid)
    } else {
      this.result
    }
  }

  def getSnapshot: WebResultUpdate = {
    val res = getResult
    webOutputMode match {
      case PaginationMode() =>
        WebPaginationUpdate(PaginationMode(), res.size, List.empty)
      case SetSnapshotMode() | SetDeltaMode() =>
        webDataFromTuple(webOutputMode, res, chartType)
    }
  }

}
