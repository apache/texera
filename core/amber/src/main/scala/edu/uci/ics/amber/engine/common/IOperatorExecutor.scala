package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

case class InputExhausted()

trait IOperatorExecutor {

  def open(): Unit

  def close(): Unit

  def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[(ITuple, Option[LinkIdentity])]

  def getParam(query: String): String = { null }

}
