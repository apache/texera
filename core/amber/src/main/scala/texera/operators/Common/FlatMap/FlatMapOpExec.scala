package texera.operators.Common.FlatMap

import Engine.Common.{InputExhausted, OperatorExecutor}
import Engine.Common.tuple.Tuple
import Engine.Common.tuple.texera.TexeraTuple
import texera.common.workflow.TexeraOperatorExecutor

class FlatMapOpExec(var flatMapFunc: (TexeraTuple => TraversableOnce[TexeraTuple]) with Serializable)
    extends TexeraOperatorExecutor
    with Serializable {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(tuple: Either[TexeraTuple, InputExhausted], input: Int): scala.Iterator[TexeraTuple] = {
    tuple match {
      case Left(t) => flatMapFunc(t).toIterator
      case Right(_) => Iterator()
    }
  }

}
