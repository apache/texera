package texera.common.operators

import Engine.Common.tuple.Tuple
import Engine.Common.{InputExhausted, OperatorExecutor}
import texera.common.tuple.TexeraTuple

trait TexeraOperatorExecutor extends OperatorExecutor {

  override def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): Iterator[Tuple] = {
    processTexeraTuple(tuple.asInstanceOf[Either[TexeraTuple, InputExhausted]], input)
  }

  def processTexeraTuple(tuple: Either[TexeraTuple, InputExhausted], input: Int): Iterator[TexeraTuple]

}
