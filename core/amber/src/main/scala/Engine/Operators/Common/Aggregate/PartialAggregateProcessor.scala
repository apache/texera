package Engine.Operators.Common.Aggregate

import Engine.Common.AmberTag.LayerTag
import Engine.Common.tuple.Tuple
import Engine.Common.TupleProcessor

import scala.collection.mutable

class PartialAggregateProcessor(val aggFunc: DistributedAggregation, val groupByKeys: Seq[Int])
    extends TupleProcessor {
  var partialObjectPerKey = new mutable.HashMap[Seq[Any], Tuple]()
  var outputIterator: Iterator[Tuple] = _

  override def accept(tuple: Tuple): Unit = {
    val key = groupByKeys.map(tuple.get)
    val partialObject = aggFunc.iterate(partialObjectPerKey.getOrElse(key, aggFunc.init()), tuple)
    partialObjectPerKey.put(key, partialObject)
  }

  override def onUpstreamChanged(from: LayerTag): Unit = {}

  override def onUpstreamExhausted(from: LayerTag): Unit = {}

  override def noMore(): Unit = {
    outputIterator = partialObjectPerKey.iterator.map(o => Tuple.fromIterable(o._1 ++ o._2.toSeq))
  }

  override def initialize(): Unit = {}

  override def hasNext: Boolean = outputIterator != null && outputIterator.hasNext

  override def next(): Tuple = outputIterator.next

  override def dispose(): Unit = {
    partialObjectPerKey = null;
    outputIterator = null;
  }

}
