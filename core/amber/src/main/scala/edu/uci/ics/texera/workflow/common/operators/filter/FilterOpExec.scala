package edu.uci.ics.texera.workflow.common.operators.filter

import edu.uci.ics.amber.engine.common.executor.OperatorExecutor
import edu.uci.ics.amber.engine.common.model.tuple.{Tuple, TupleLike}

abstract class FilterOpExec extends OperatorExecutor with Serializable {

  var filterFunc: Tuple => Boolean = _

  def setFilterFunc(func: Tuple => java.lang.Boolean): Unit =
    filterFunc = (tuple: Tuple) => func.apply(tuple).booleanValue()

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
    if (filterFunc(tuple)) Iterator.single(tuple) else Iterator.empty

}
