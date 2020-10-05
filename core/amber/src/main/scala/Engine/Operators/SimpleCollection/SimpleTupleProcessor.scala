package Engine.Operators.SimpleCollection

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.{TableMetadata, TupleProcessor}

class SimpleTupleProcessor extends TupleProcessor {
  var _tuple: Tuple = _
  var nextFlag = false

  override def accept(tuple: Tuple): Unit = {
    _tuple = tuple
    nextFlag = true
  }

  override def noMore(): Unit = {}

  override def hasNext: Boolean = nextFlag

  override def next(): Tuple = {
    nextFlag = false
    _tuple
  }

  override def updateParamMap(): Unit = {}

  override def dispose(): Unit = {}

  override def initializeWorker(): Unit = {}

  override def onUpstreamChanged(from: LayerTag): Unit = {}

  override def onUpstreamExhausted(from: LayerTag): Unit = {}
}
