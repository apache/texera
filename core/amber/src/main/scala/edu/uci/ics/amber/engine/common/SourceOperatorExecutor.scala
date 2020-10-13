package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.Tuple

trait SourceOperatorExecutor extends OperatorExecutor {

  override def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): Iterator[Tuple] = {
    throw new UnsupportedOperationException()
  }

  def produce(): Iterator[Tuple]

}
