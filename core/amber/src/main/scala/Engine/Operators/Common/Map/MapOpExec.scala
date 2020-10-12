package Engine.Operators.Common.Map

import Engine.Common.tuple.Tuple
import Engine.Common.{InputExhausted, OperatorExecutor}

class MapOpExec[T <: Tuple](var mapFunc: (T => T) with Serializable) extends OperatorExecutor with Serializable {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): scala.Iterator[Tuple] = {
    tuple match {
      case Left(t) => Iterator(mapFunc(t.asInstanceOf[T]))
      case Right(_) => Iterator()
    }
  }
}
