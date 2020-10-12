package texera.common.operators.map

import Engine.Common.InputExhausted
import texera.common.operators.TexeraOperatorExecutor
import texera.common.tuple.TexeraTuple

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
