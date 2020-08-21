package Engine.Operators.Sink

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.TupleProcessor

import scala.collection.mutable

class SimpleSinkProcessor extends TupleProcessor{

  val results: mutable.MutableList[Tuple] = mutable.MutableList()

  override def accept(tuple: Tuple): Unit = {
    println("Sink: "+ tuple.toString)
    results += tuple
  }

  def getResultTuples(): mutable.MutableList[Tuple] = {
    results
  }

  override def noMore(): Unit = {

  }

  override def initialize(): Unit = {

  }

  override def hasNext: Boolean = false

  override def next(): Tuple = {
    throw new NotImplementedError()
  }

  override def dispose(): Unit = {

  }

  override def onUpstreamChanged(from: LayerTag): Unit = {

  }

  override def onUpstreamExhausted(from: LayerTag): Unit = {

  }
}
