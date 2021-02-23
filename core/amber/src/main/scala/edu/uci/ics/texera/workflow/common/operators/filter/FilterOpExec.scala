package edu.uci.ics.texera.workflow.common.operators.filter

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

abstract class FilterOpExec() extends OperatorExecutor with Serializable {

  var filterFunc: Tuple => java.lang.Boolean = _

  def setFilterFunc(func: Tuple => java.lang.Boolean): Unit = {
    this.filterFunc = func
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t)  => if (filterFunc(t)) Iterator(t) else Iterator()
      case Right(_) => Iterator()
    }
  }

}
