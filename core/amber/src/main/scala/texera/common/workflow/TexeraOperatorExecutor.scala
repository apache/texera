package texera.common.workflow

import Engine.Common.tuple.Tuple
import Engine.Common.tuple.texera.TexeraTuple
import Engine.Common.{InputExhausted, OperatorExecutor}

trait TexeraOperatorExecutor extends OperatorExecutor {

  override def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): Iterator[Tuple] = {
    processTexeraTuple(tuple.asInstanceOf[Either[TexeraTuple, InputExhausted]], input)
  }

  def processTexeraTuple(tuple: Either[TexeraTuple, InputExhausted], input: Int): Iterator[TexeraTuple]

}
