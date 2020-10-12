package Engine.Operators.Sort

import Engine.Common.AmberTag.LayerTag
import Engine.Common.tuple.Tuple
import Engine.Common.{TableMetadata, TupleProcessor}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SortTupleProcessor[T: Ordering](val targetField: Int) extends TupleProcessor {
  var results: ArrayBuffer[Tuple] = _
  var iter: Iterator[Tuple] = _

  var params: mutable.HashMap[String,String] = new mutable.HashMap[String,String]

  override def accept(tuple: Tuple): Unit = {
    results.append(tuple)
  }

  override def noMore(): Unit = {
    iter = results.sortBy(x => x.getAs[T](targetField)).iterator
  }

  def updateParamMap(): Unit = {}

  override def hasNext: Boolean = iter != null && iter.hasNext

  override def next(): Tuple = {
    iter.next()
  }

  override def dispose(): Unit = {
    results = null
    iter = null
  }

  override def initialize(): Unit = {
    results = new ArrayBuffer[Tuple]()
    updateParamMap()
  }

  override def getParam(query: String): String = {return params.getOrElse(query,null)}

  override def onUpstreamChanged(from: LayerTag): Unit = {}

  override def onUpstreamExhausted(from: LayerTag): Unit = {}
}
