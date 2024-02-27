package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

trait ISinkOperatorExecutor extends IOperatorExecutor {

  override def processTupleMultiPort(
      tuple: Either[ITuple, InputExhausted],
      port: Int
  ): Iterator[(ITuple, Option[PortIdentity])] = {
    consume(tuple, port)
    Iterator.empty
  }

  def consume(tuple: Either[ITuple, InputExhausted], input: Int): Unit
}
