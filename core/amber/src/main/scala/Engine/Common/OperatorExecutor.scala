package Engine.Common

import Engine.Common.tuple.Tuple

case class InputExhausted()

trait OperatorExecutor {

  def open(): Unit

  def close(): Unit

  def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): Iterator[Tuple]

}
