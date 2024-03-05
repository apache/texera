package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait ISinkOperatorExecutor extends IOperatorExecutor {

  override def processTupleMultiPort(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[(Tuple, Option[PortIdentity])] = {
    consume(tuple, port)
    Iterator.empty
  }

  def consume(tuple: Either[Tuple, InputExhausted], input: Int): Unit
}
