package Engine.Operators.Filter

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.{TableMetadata, TupleProcessor}

import scala.collection.mutable

class FilterTupleProcessor[T: Ordering](
    val targetField: Int,
    val filterType: FilterType.Val[T],
    val threshold: T
) extends TupleProcessor {
  var _tuple: Tuple = _
  var nextFlag = false
  var params: mutable.HashMap[String,String] = new mutable.HashMap[String,String]()

  override def accept(tuple: Tuple): Unit = {
    if (filterType.validate(tuple.getAs(targetField), threshold)) {
      nextFlag = true
      _tuple = tuple
    }
  }

  override def noMore(): Unit = {}

  override def hasNext: Boolean = nextFlag

  override def next(): Tuple = {
    nextFlag = false
    _tuple
  }

  override def dispose(): Unit = {}

  def updateParamMap(): Unit = {
    params += ("targetField"->Integer.toString(targetField), "filterType"->filterType.getClass().getName(), "threshold"->threshold.toString())
  }

  override def getParam(query: String): String = {params.getOrElse(query,null)}

  override def initialize(): Unit = {updateParamMap()}

  override def onUpstreamChanged(from: LayerTag): Unit = {}

  override def onUpstreamExhausted(from: LayerTag): Unit = {}
}
