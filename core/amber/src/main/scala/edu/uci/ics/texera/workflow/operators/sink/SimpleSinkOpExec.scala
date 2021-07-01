package edu.uci.ics.texera.workflow.operators.sink

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.engine.common.{ITupleSinkOperatorExecutor, InputExhausted}
import edu.uci.ics.texera.workflow.common.{IncrementalOutputMode, ProgressiveUtils}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

import scala.collection.mutable
import IncrementalOutputMode._

class SimpleSinkOpExec(
    val operatorSchemaInfo: OperatorSchemaInfo,
    val outputMode: IncrementalOutputMode,
    val chartType: Option[String]
) extends ITupleSinkOperatorExecutor {

  val results: mutable.ListBuffer[Tuple] = mutable.ListBuffer()

  def getResultTuples(): List[ITuple] = {
    outputMode match {
      case SET_SNAPSHOT =>
        results.toList
      case SET_DELTA =>
        val ret = results.toList
        // clear the delta result buffer after every progressive output
        results.clear()
        ret
    }
  }

  override def getOutputMode(): IncrementalOutputMode = this.outputMode

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: LinkIdentity
  ): scala.Iterator[ITuple] = {
    tuple match {
      case Left(t) =>
        outputMode match {
          case SET_SNAPSHOT =>
            updateSetSnapshot(t.asInstanceOf[Tuple])
            Iterator()
          case SET_DELTA =>
            results += t.asInstanceOf[Tuple]
            Iterator()
        }
      case Right(_) =>
        Iterator()
    }
  }

  private def updateSetSnapshot(deltaUpdate: Tuple): Unit = {
    val (isInsertion, tupleValue) =
      ProgressiveUtils.getTupleFlagAndValue(deltaUpdate, operatorSchemaInfo)
    if (isInsertion) {
      results += tupleValue
    } else {
      results -= tupleValue
    }
  }

}
