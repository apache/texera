package edu.uci.ics.texera.workflow.operators.sink

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.engine.common.{ITupleSinkOperatorExecutor, InputExhausted}
import edu.uci.ics.texera.workflow.common.ProgressiveUtils
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

import scala.collection.mutable

class SimpleSinkOpExec(val operatorSchemaInfo: OperatorSchemaInfo)
    extends ITupleSinkOperatorExecutor {

  val results: mutable.ListBuffer[Tuple] = mutable.ListBuffer()

  def getResultTuples(): List[ITuple] = {
    results.toList
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: LinkIdentity
  ): scala.Iterator[ITuple] = {
    tuple match {
      case Left(t) =>
        updateResult(t.asInstanceOf[Tuple])
        Iterator()
      case Right(_) =>
        Iterator()
    }
  }

  private def updateResult(tuple: Tuple): Unit = {
    val (isInsertion, tupleValue) = ProgressiveUtils.getTupleFlagAndValue(tuple, operatorSchemaInfo)
    if (isInsertion) {
      results += tupleValue
    } else {
      results -= tupleValue
    }
  }

}
