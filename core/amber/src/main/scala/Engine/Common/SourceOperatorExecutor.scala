package Engine.Common

import Engine.Common.tuple.Tuple

trait SourceOperatorExecutor extends OperatorExecutor {

  override def inputExhausted(input: Int): Iterator[Tuple] = { Iterator() }

  override def processTuple(tuple: Tuple, input: Int): Iterator[Tuple] = {
    throw new UnsupportedOperationException()
  }

  def produce(): Iterator[Tuple]

}
