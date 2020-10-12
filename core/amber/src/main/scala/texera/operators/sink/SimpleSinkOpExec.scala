package texera.operators.sink

import Engine.Common.TupleSink
import Engine.Common.tuple.Tuple

import scala.collection.mutable

class SimpleSinkOpExec extends TupleSink {

  val results: mutable.MutableList[Tuple] = mutable.MutableList()

  def getResultTuples(): Array[Tuple] = {
    results.toArray
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(tuple: Tuple, input: Int): scala.Iterator[Tuple] = {
    System.out.println(tuple)
    this.results += tuple
    Iterator()
  }
  override def inputExhausted(input: Int): Iterator[Tuple] = { Iterator() }

}
