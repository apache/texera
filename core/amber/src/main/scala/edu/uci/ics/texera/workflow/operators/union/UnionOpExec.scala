package edu.uci.ics.texera.workflow.operators.union

class UnionOpExec extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    Iterator(tuple)
  }
}
