package edu.uci.ics.texera.workflow.operators.union

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class UnionOpExec extends OperatorExecutor {
  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t)  => Iterator(t)
      case Right(_) => Iterator()
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
