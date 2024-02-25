package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

trait ISinkOperatorExecutor extends IOperatorExecutor {

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: Int
  ): Iterator[(ITuple, Option[PortIdentity])] = {
    consume(tuple, input)
    Iterator.empty
  }

  def consume(tuple: Either[ITuple, InputExhausted], input: Int): Unit
}
