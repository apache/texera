package edu.uci.ics.amber.operator.filter

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

abstract class FilterOpExec extends OperatorExecutor with Serializable {

  var filterFunc: Tuple => Boolean = _

  def setFilterFunc(func: Tuple => Boolean): Unit =
    filterFunc = func

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
    if (filterFunc(tuple)) Iterator.single(tuple) else Iterator.empty

}
