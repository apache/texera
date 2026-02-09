package org.apache.texera.amber.operator.loop

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}

class LoopEndOpExec extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator(tuple)
}
