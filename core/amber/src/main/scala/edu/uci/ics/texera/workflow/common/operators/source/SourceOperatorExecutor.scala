package edu.uci.ics.texera.workflow.common.operators.source

import edu.uci.ics.amber.engine.common.ISourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait SourceOperatorExecutor extends ISourceOperatorExecutor {

  override def produceTuple(): Iterator[Tuple] = {
    produceTexeraTuple()
  }

  def produceTexeraTuple(): Iterator[Tuple]

}
