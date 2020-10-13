package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.common.tuple.Tuple
import edu.uci.ics.amber.engine.common.{InputExhausted, OperatorExecutor}
import edu.uci.ics.texera.workflow.common.tuple.TexeraTuple

trait TexeraOperatorExecutor extends OperatorExecutor {

  override def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): Iterator[Tuple] = {
    processTexeraTuple(tuple.asInstanceOf[Either[TexeraTuple, InputExhausted]], input)
  }

  def processTexeraTuple(tuple: Either[TexeraTuple, InputExhausted], input: Int): Iterator[TexeraTuple]

}
