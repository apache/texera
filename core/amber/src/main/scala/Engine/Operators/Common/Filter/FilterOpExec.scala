package Engine.Operators.Common.Filter

import Engine.Common.tuple.Tuple
import Engine.Common.{InputExhausted, OperatorExecutor}
import Engine.Common.tuple.texera.TexeraTuple

class FilterOpExec[T <: Tuple](var filterFunc: (T => Boolean) with Serializable)
    extends OperatorExecutor
    with Serializable {

  override def open(): Unit = {}

  override def close(): Unit = {}

  def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): Iterator[Tuple] = {
    tuple match {
      case Left(t) => if (filterFunc(t.asInstanceOf[T])) Iterator(t) else Iterator()
      case Right(_) => Iterator()
    }
  }

}
