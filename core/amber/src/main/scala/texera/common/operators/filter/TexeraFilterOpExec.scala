package texera.common.operators.filter

import engine.common.InputExhausted
import texera.common.operators.TexeraOperatorExecutor
import texera.common.tuple.TexeraTuple

abstract class TexeraFilterOpExec()
    extends TexeraOperatorExecutor
    with Serializable {

  var filterFunc: TexeraTuple => java.lang.Boolean = _

  def setFilterFunc(func: TexeraTuple => java.lang.Boolean): Unit = {
    this.filterFunc = func
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(tuple: Either[TexeraTuple, InputExhausted], input: Int): Iterator[TexeraTuple] = {
    tuple match {
      case Left(t) => if (filterFunc(t)) Iterator(t) else Iterator()
      case Right(_) => Iterator()
    }
  }

}
