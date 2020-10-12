package Engine.Operators.SimpleCollection

import Engine.Common.AmberTag.LayerTag
import Engine.Common.tuple.Tuple
import Engine.Common.{TableMetadata, TupleProcessor}

import scala.collection.mutable

class SimpleTupleProcessor extends TupleProcessor {
  var _tuple: Tuple = _
  var nextFlag = false

  var params: mutable.HashMap[String,String] = new mutable.HashMap[String,String]

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

  def updateParamMap(): Unit = {}

  override def dispose(): Unit = {}

  override def initialize(): Unit = {updateParamMap()}

  override def getParam(query: String): String = {return params.getOrElse(query,null)}

  override def onUpstreamChanged(from: LayerTag): Unit = {}

  override def onUpstreamExhausted(from: LayerTag): Unit = {}
}
