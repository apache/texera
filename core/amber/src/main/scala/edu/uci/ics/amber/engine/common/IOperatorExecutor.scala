package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

case class InputExhausted()

trait IOperatorExecutor {

  def open(): Unit

  def close(): Unit

  def processTupleMultiPort(
      tuple: Either[ITuple, InputExhausted],
      input: Int
  ): Iterator[(TupleLike, Option[PortIdentity])]

  def getParam(query: String): String = { null }

}
