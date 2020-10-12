package texera.common.operators

import Engine.Common.SourceOperatorExecutor
import Engine.Common.tuple.Tuple
import Engine.Common.tuple.texera.TexeraTuple

trait TexeraSourceOperatorExecutor extends SourceOperatorExecutor {

  override def produce(): Iterator[Tuple] = {
    produceTexeraTuple()
  }

  def produceTexeraTuple(): Iterator[TexeraTuple]

}
