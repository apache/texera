package edu.uci.ics.texera.workflow.operators.intersect

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

class IntersectOpExec extends OperatorExecutor {
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
    if (exhaustedCounter == 2) {
      // both streams are exhausted, take the intersect and return the results
      leftSet.intersect(rightSet).iterator
    } else {
      // only one of the stream is exhausted, continue accepting tuples
      Iterator()
    }
  }
}
