package Engine.Common

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple

trait GenericTupleProcessor[T] extends GenericTupleProducer[T] {

  def accept(tuple:T): Unit

  def onUpstreamChanged(from:LayerTag):Unit

  def onUpstreamExhausted(from:LayerTag):Unit

  def noMore():Unit

}
