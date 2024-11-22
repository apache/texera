package edu.uci.ics.texera.workflow.operators.limit

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.core.tuple.Tuple

class LimitOpExec(limit: Int) extends OperatorExecutor {
  var count = 0

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    if (count < limit) {
      count += 1
      Iterator(tuple)
    } else {
      Iterator()
    }

  }

}
