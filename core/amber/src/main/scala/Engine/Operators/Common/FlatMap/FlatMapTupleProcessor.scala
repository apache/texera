package Engine.Operators.Common.FlatMap

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.TupleProcessor

import scala.collection.Iterator

class FlatMapTupleProcessor(
    val flatMapFunc: (Tuple => TraversableOnce[Tuple]) with java.io.Serializable
) extends TupleProcessor {

  private var tupleIterator: Iterator[Tuple] = _
  private var nextFlag = false

  override def accept(tuple: Tuple): Unit = {
    this.tupleIterator = flatMapFunc.apply(tuple).toIterator
    this.nextFlag = tupleIterator.hasNext
  }

  override def onUpstreamChanged(from: LayerTag): Unit = {}

  override def onUpstreamExhausted(from: LayerTag): Unit = {}

  override def noMore(): Unit = {}

  override def initialize(): Unit = {}

  override def hasNext: Boolean = nextFlag

  override def next(): Tuple = {
    val next = tupleIterator.next
    this.nextFlag = tupleIterator.hasNext
    next
  }

  override def dispose(): Unit = {}
}
