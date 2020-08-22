package Engine.Common

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple

trait TupleProcessor extends TupleProducer {
  @throws(classOf[Exception])
  def accept(tuple: Tuple): Unit

  def onUpstreamChanged(from: LayerTag): Unit

  def onUpstreamExhausted(from: LayerTag): Unit

  def noMore(): Unit
}
