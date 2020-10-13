package texera.common.operators.source

import Engine.Common.SourceOperatorExecutor
import Engine.Common.tuple.Tuple
import texera.common.tuple.TexeraTuple

trait TexeraSourceOperatorExecutor extends SourceOperatorExecutor {

  override def produce(): Iterator[Tuple] = {
    produceTexeraTuple()
  }

  def produceTexeraTuple(): Iterator[TexeraTuple]

}
