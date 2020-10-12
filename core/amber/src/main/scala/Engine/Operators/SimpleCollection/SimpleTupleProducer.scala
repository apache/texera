package Engine.Operators.SimpleCollection

import Engine.Common.tuple.Tuple
import Engine.Common.TupleProducer

import scala.collection.mutable

class SimpleTupleProducer(val limit: Int, val delay: Int = 0) extends TupleProducer {

  var current = 0
  override def hasNext: Boolean = current < limit

  var params: mutable.HashMap[String,String] = new mutable.HashMap[String,String]

  override def next(): Tuple = {
    current += 1
    if (delay > 0) {
      Thread.sleep(delay)
    }
    Tuple(current)
  }

  override def dispose(): Unit = {}

  def updateParamMap(): Unit = {}

  override def initialize(): Unit = {updateParamMap()}

  override def getParam(query: String): String = {return params.getOrElse(query,null)}
}
