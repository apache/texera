package edu.uci.ics.texera.workflow.operators.limit

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
