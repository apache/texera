package edu.uci.ics.texera.workflow.operators.sink

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.engine.common.{ITupleSinkOperatorExecutor, InputExhausted}

import scala.collection.mutable

class SimpleSinkOpExec extends ITupleSinkOperatorExecutor {

  val results: mutable.MutableList[ITuple] = mutable.MutableList()

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
        this.results += t
        Iterator()
      case Right(_) =>
        Iterator()
    }
  }

}
