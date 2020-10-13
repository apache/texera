package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.Tuple

case class InputExhausted()

trait OperatorExecutor {

  def open(): Unit

  def close(): Unit

  def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): Iterator[Tuple]

  def getParam(query:String): String = { null }

}
