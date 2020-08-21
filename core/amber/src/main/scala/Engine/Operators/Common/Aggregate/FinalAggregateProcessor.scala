package Engine.Operators.Common.Aggregate

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.TupleProcessor

import scala.collection.mutable

class FinalAggregateProcessor(val aggFunc: DistributedAggregation, val groupByKeys: Seq[Int]) extends TupleProcessor {

  var partialObjectPerKey = new mutable.HashMap[Seq[Any], Tuple]()
  var outputIterator: Iterator[Tuple] = _

  override def accept(tuple: Tuple): Unit = ???

  override def onUpstreamChanged(from: LayerTag): Unit = ???

  override def onUpstreamExhausted(from: LayerTag): Unit = ???

  override def noMore(): Unit = ???

  override def initialize(): Unit = ???

  override def hasNext: Boolean = ???

  override def next(): Tuple = ???

  override def dispose(): Unit = ???
}
