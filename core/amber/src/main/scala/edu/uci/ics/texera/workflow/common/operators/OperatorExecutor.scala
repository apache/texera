package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted}
import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait OperatorExecutor extends IOperatorExecutor {

  override def processTuple(tuple: Either[ITuple, InputExhausted], input: Int): Iterator[ITuple] = {
    processTexeraTuple(tuple.asInstanceOf[Either[Tuple, InputExhausted]], input)
  }

  def processTexeraTuple(tuple: Either[Tuple, InputExhausted], input: Int): Iterator[Tuple]

}
