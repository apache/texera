package texera.common.operators.filter

import Engine.Common.InputExhausted
import texera.common.operators.TexeraOperatorExecutor
import texera.common.tuple.TexeraTuple

class TexeraFilterOpExec(var filterFunc: TexeraTuple => java.lang.Boolean)
    extends TexeraOperatorExecutor
    with Serializable {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(tuple: Either[TexeraTuple, InputExhausted], input: Int): Iterator[TexeraTuple] = {
    tuple match {
      case Left(t) => if (filterFunc(t)) Iterator(t) else Iterator()
      case Right(_) => Iterator()
    }
  }

}
