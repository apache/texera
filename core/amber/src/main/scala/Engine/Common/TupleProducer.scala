package Engine.Common

import Engine.Common.tuple.Tuple

import scala.collection.mutable
import scala.collection.mutable.HashMap

trait TupleProducer {

  @throws(classOf[Exception])
  def initialize(): Unit

  @throws(classOf[Exception])
  def hasNext: Boolean

  @throws(classOf[Exception])
  def next(): Tuple

  @throws(classOf[Exception])
  def dispose(): Unit

  @throws(classOf[Exception])
  def getParam(query:String): String

}
