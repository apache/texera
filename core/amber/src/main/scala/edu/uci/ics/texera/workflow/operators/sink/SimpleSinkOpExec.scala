package edu.uci.ics.texera.workflow.operators.sink

import edu.uci.ics.amber.engine.common.tuple.Tuple
import edu.uci.ics.amber.engine.common.{InputExhausted, TupleSinkOperatorExecutor}

import scala.collection.mutable

class SimpleSinkOpExec extends TupleSinkOperatorExecutor {

  val results: mutable.MutableList[Tuple] = mutable.MutableList()

  def getResultTuples(): Array[Tuple] = {
    results.toArray
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): scala.Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        this.results += t
        Iterator()
      case Right(_) =>
        Iterator()
    }
  }

}
