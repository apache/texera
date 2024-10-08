package edu.uci.ics.texera.workflow.operators.symmetricDifference

import edu.uci.ics.amber.engine.common.executor.OperatorExecutor
import edu.uci.ics.amber.engine.common.model.tuple.{Tuple, TupleLike}

import scala.collection.mutable

class SymmetricDifferenceOpExec extends OperatorExecutor {
  private val leftSet = new mutable.HashSet[Tuple]()
  private val rightSet = new mutable.HashSet[Tuple]()

  private var exhaustedCounter: Int = 0

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    // add the tuple to corresponding set
    if (port == 0) leftSet += tuple else rightSet += tuple
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    exhaustedCounter += 1
    if (2 == exhaustedCounter) {
      // both streams are exhausted, take the intersect and return the results
      leftSet.union(rightSet).diff(leftSet.intersect(rightSet)).iterator
    } else {
      // only one of the stream is exhausted, continue accepting tuples
      Iterator()
    }
  }
}
