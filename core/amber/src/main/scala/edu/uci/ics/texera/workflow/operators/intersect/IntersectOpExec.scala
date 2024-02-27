package edu.uci.ics.texera.workflow.operators.intersect

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

class IntersectOpExec extends OperatorExecutor {
  private val leftSet = new mutable.HashSet[Tuple]()
  private val rightSet = new mutable.HashSet[Tuple]()

  private var exhaustedCounter: Int = 0

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] = {
    if (port >= 2) {
      throw new IllegalArgumentException("input port should not be more than 2")
    }
    tuple match {
      case Left(t) =>
        // add the tuple to corresponding set
        if (port == 0) leftSet += t else rightSet += t
        Iterator()

      case Right(_) =>
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

  override def open(): Unit = {}

  override def close(): Unit = {}
}
