package Engine.Operators.Common.Map

import Engine.Common.AmberTag.LayerTag
import Engine.Common.tuple.Tuple
import Engine.Common.TupleProcessor
import scala.Function1
import scala.Serializable

class MapTupleProcessor(
    val mapFunc: (Tuple => Tuple) with java.io.Serializable
) extends TupleProcessor {

  private var tuple: Tuple = _
  private var nextFlag = false

  override def accept(tuple: Tuple): Unit = {
    nextFlag = true
    this.tuple = mapFunc.apply(tuple)
  }

  override def onUpstreamChanged(from: LayerTag): Unit = {}

  override def onUpstreamExhausted(from: LayerTag): Unit = {}

  override def noMore(): Unit = {}

  override def getParam(query: String): String = {return null}

  override def initialize(): Unit = {}

  override def hasNext: Boolean = nextFlag

  override def next(): Tuple = {
    nextFlag = false
    tuple
  }

  override def dispose(): Unit = {}
}
