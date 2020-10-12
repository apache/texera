package texera.operators.Common.Filter

import Engine.Common.InputExhausted
import Engine.Common.tuple.texera.TexeraTuple
import texera.common.workflow.TexeraOperatorExecutor

class FilterOpExec(var filterFunc: (TexeraTuple => Boolean) with Serializable)
    extends TexeraOperatorExecutor
    with Serializable {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(tuple: Either[TexeraTuple, InputExhausted], input: Int): Iterator[TexeraTuple] = {
    tuple match {
      case Left(t) => if (filterFunc(t)) Iterator(t) else Iterator()
      case Right(_) => Iterator()
    }
  }

}
