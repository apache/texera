package edu.uci.ics.texera.workflow.operators.union

import edu.uci.ics.amber.engine.common.model.tuple.{Tuple, TupleLike}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor

class UnionOpExec extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    Iterator(tuple)
  }
}
