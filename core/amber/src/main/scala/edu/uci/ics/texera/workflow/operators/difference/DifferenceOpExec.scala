package edu.uci.ics.texera.workflow.operators.difference

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

class DifferenceOpExec extends OperatorExecutor {

  private val leftHashSet: mutable.HashSet[Tuple] = new mutable.HashSet()
  private val rightHashSet: mutable.HashSet[Tuple] = new mutable.HashSet()
  private var exhaustedCounter: Int = 0

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (port == 1) { // right input
      rightHashSet.add(tuple)
    } else { // left input
      leftHashSet.add(tuple)
    }
    Iterator()

  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    exhaustedCounter += 1
    if (2 == exhaustedCounter) {
      leftHashSet.diff(rightHashSet).iterator
    } else {
      Iterator()
    }
  }

}
