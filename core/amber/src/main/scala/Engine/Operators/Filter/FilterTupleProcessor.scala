package Engine.Operators.Filter

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.{TableMetadata, TupleProcessor}


class FilterTupleProcessor[T:Ordering](val targetField:Int, val filterType:FilterType.Val[T], val threshold:T) extends TupleProcessor {
  var _tuple:Tuple = _
  var nextFlag = false

  override def accept(tuple: Tuple): Unit = {
    if (filterType.validate(tuple.getAs(targetField),threshold)) {
      nextFlag = true
      _tuple = tuple
    }
  }

  override def noMore(): Unit = {

  }

  override def hasNext: Boolean = nextFlag

  override def next(): Tuple = {
    nextFlag = false
    _tuple
  }

  override def dispose(): Unit = {

  }

  override def initialize(): Unit = {

  }

  override def onUpstreamChanged(from: LayerTag): Unit = {

  }

  override def onUpstreamExhausted(from: LayerTag): Unit = {

  }
}

