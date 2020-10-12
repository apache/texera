package texera.operators.sink

import Engine.Common.{InputExhausted, TupleSink}
import Engine.Common.tuple.Tuple

import scala.collection.mutable

class SimpleSinkOpExec extends TupleSink {

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
