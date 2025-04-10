package edu.uci.ics.amber.operator.difference

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

import scala.collection.mutable

class DifferenceOpExec extends OperatorExecutor {
  private var leftHashSet: mutable.HashSet[Tuple] = _
  private var rightHashSet: mutable.HashSet[Tuple] = _
  private var exhaustedCounter: Int = _

  override def open(): Unit = {
    leftHashSet = new mutable.HashSet()
    rightHashSet = new mutable.HashSet()
    exhaustedCounter = 0
  }

  override def close(): Unit = {
    leftHashSet.clear()
    rightHashSet.clear()
  }

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
