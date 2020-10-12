package Engine.Operators.Common.Aggregate

import Engine.Common.AmberTag.LayerTag
import Engine.Common.tuple.Tuple
import Engine.Common.OperatorExecutor

import scala.collection.mutable

class FinalAggregateProcessor[Partial <: AnyRef, Final <: AnyRef](val aggFunc: DistributedAggregation[Partial, Final], val groupByKeys: Seq[String])
    extends OperatorExecutor {

  var partialObjectPerKey = new mutable.HashMap[Seq[Any], Tuple]()
  var outputIterator: Iterator[Tuple] = _

override def open(): Unit = {}
override def close(): Unit = {
}
override def processTuple(tuple: Tuple, input:  Int): scala.Iterator[Tuple] = {
null
}
override def inputExhausted(input:  Int): Iterator[Tuple] = { null }

}

