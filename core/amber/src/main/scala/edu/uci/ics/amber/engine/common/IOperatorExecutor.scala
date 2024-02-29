package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

case class InputExhausted()

trait IOperatorExecutor {

  def open(): Unit

  def close(): Unit

  def processTupleMultiPort(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])]

}
