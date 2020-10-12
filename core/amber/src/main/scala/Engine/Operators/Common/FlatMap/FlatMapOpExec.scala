package Engine.Operators.Common.FlatMap

import Engine.Common.{InputExhausted, OperatorExecutor}
import Engine.Common.tuple.Tuple

class FlatMapOpExec[T <: Tuple](var flatMapFunc: (T => TraversableOnce[T]) with Serializable)
    extends OperatorExecutor
    with Serializable {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): scala.Iterator[Tuple] = {
    tuple match {
      case Left(t) => flatMapFunc(t.asInstanceOf[T]).toIterator
      case Right(_) => Iterator()
    }
  }

}
