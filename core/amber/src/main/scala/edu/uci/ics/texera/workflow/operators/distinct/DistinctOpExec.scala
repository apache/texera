package edu.uci.ics.texera.workflow.operators.distinct

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

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
