package Engine.Common

import Engine.Common.AmberTuple.Tuple

trait TupleProducer {

  @throws(classOf[Exception])
  def initialize(): Unit

  @throws(classOf[Exception])
  def hasNext: Boolean

  @throws(classOf[Exception])
  def next(): Tuple

  @throws(classOf[Exception])
  def dispose(): Unit

}
