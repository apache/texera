package Engine.Common

import Engine.Common.tuple.Tuple

trait SourceOperatorExecutor {

  @throws(classOf[Exception])
  def initialize(): Unit

  @throws(classOf[Exception])
  def hasNext: Boolean

  @throws(classOf[Exception])
  def next(): Tuple

  @throws(classOf[Exception])
  def dispose(): Unit

}
