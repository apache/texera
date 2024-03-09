package edu.uci.ics.texera.workflow.common.operators.filter

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

abstract class FilterOpExec extends OperatorExecutor with Serializable {

  var filterFunc: Tuple => java.lang.Boolean = _

  def setFilterFunc(func: Tuple => java.lang.Boolean): Unit = {
    this.filterFunc = func
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (filterFunc(tuple)) Iterator(tuple) else Iterator()
  }

}
