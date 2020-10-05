package Engine.Operators.SimpleCollection

import Engine.Common.AmberTuple.{AmberTuple, Tuple}
import Engine.Common.TupleProducer

class SimpleTupleProducer(val limit: Int, val delay: Int = 0) extends TupleProducer {

  var current = 0
  override def hasNext: Boolean = current < limit

  override def next(): Tuple = {
    current += 1
    if (delay > 0) {
      Thread.sleep(delay)
    }
    Tuple(current)
  }

  override def dispose(): Unit = {}

  override def initialize(): Unit = {}
}
