package Engine.Common

trait GenericTupleProducer[T] {

  def initialize(): Unit

  def hasNext: Boolean

  def next(): T

  def dispose(): Unit

}
