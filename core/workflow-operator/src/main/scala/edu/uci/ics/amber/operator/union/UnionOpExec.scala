package edu.uci.ics.amber.operator.union

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

class UnionOpExec extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    Iterator(tuple)
  }
}
