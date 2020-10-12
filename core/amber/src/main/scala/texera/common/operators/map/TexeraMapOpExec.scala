package texera.common.operators.map

import Engine.Common.InputExhausted
import Engine.Common.tuple.texera.TexeraTuple
import texera.common.workflow.TexeraOperatorExecutor

class TexeraMapOpExec(var mapFunc: (TexeraTuple => TexeraTuple) with Serializable)
    extends TexeraOperatorExecutor
    with Serializable {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[TexeraTuple, InputExhausted],
      input: Int
  ): Iterator[TexeraTuple] = {
    tuple match {
      case Left(t)  => Iterator(mapFunc(t))
      case Right(_) => Iterator()
    }
  }
}
