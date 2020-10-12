package texera.common.operators.flatmap

import Engine.Common.InputExhausted
import texera.common.operators.TexeraOperatorExecutor
import texera.common.tuple.TexeraTuple

class TexeraFlatMapOpExec(
    var flatMapFunc: (TexeraTuple => TraversableOnce[TexeraTuple]) with Serializable
) extends TexeraOperatorExecutor
    with Serializable {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[TexeraTuple, InputExhausted],
      input: Int
  ): Iterator[TexeraTuple] = {
    tuple match {
      case Left(t)  => flatMapFunc(t).toIterator
      case Right(_) => Iterator()
    }
  }

}
