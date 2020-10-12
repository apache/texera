package texera.common.workflow

import Engine.Common.SourceOperatorExecutor
import Engine.Common.tuple.texera.TexeraTuple

trait TexeraSourceOperatorExecutor extends SourceOperatorExecutor {

  override def produce(): Iterator[TexeraTuple] = {
    produceTexeraTuple()
  }

  def produceTexeraTuple(): Iterator[TexeraTuple]

}
