package edu.uci.ics.texera.workflow.operators.limit

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class LimitOpExec(val limit: Int) extends OperatorExecutor {
  var count = 0

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        if (count < limit) {
          count += 1
          Iterator(t)
        } else {
          Iterator()
        }
      case Right(_) => Iterator()
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
