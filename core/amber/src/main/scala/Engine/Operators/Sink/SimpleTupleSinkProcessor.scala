package Engine.Operators.Sink

import Engine.Common.AmberTag.LayerTag
import Engine.Common.tuple.Tuple
import Engine.Common.{TupleSink, TupleProcessor}

import scala.collection.mutable

class SimpleTupleSinkProcessor extends TupleSink {

  val results: mutable.MutableList[Tuple] = mutable.MutableList()
  var params: mutable.HashMap[String,String] = new mutable.HashMap[String,String]

  override def accept(tuple: Tuple): Unit = {
    println("Sink: " + tuple.toString)
    results += tuple
  }

  def getResultTuples(): Array[Tuple] = {
    results.toArray
  }

  override def noMore(): Unit = {}

  def updateParamMap(): Unit = {}

  override def initialize(): Unit = {updateParamMap()}

  override def getParam(query: String): String = {return params.getOrElse(query,null)}

  override def hasNext: Boolean = false

  override def next(): Tuple = {
    throw new NotImplementedError()
  }

  override def dispose(): Unit = {}

  override def onUpstreamChanged(from: LayerTag): Unit = {}

  override def onUpstreamExhausted(from: LayerTag): Unit = {}
}
