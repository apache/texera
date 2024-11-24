package edu.uci.ics.amber.operator.distinct

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import scala.collection.mutable

/**
  * An executor for the distinct operation that filters out duplicate tuples.
  * It uses a `LinkedHashSet` to preserve the input order while removing duplicates.
  */
class DistinctOpExec extends OperatorExecutor {
  private val seenTuples: mutable.LinkedHashSet[Tuple] = mutable.LinkedHashSet()
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    seenTuples.add(tuple)
    Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    seenTuples.iterator
  }

}
