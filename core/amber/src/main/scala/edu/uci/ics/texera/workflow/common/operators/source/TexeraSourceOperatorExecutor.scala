package edu.uci.ics.texera.workflow.common.operators.source

import edu.uci.ics.amber.engine.common.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.TexeraTuple

trait TexeraSourceOperatorExecutor extends SourceOperatorExecutor {

  override def produce(): Iterator[Tuple] = {
    produceTexeraTuple()
  }

  def produceTexeraTuple(): Iterator[TexeraTuple]

}
