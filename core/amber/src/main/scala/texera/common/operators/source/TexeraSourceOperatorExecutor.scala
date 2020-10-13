package texera.common.operators.source

import engine.common.SourceOperatorExecutor
import engine.common.tuple.Tuple
import texera.common.tuple.TexeraTuple

trait TexeraSourceOperatorExecutor extends SourceOperatorExecutor {

  override def produce(): Iterator[Tuple] = {
    produceTexeraTuple()
  }

  def produceTexeraTuple(): Iterator[TexeraTuple]

}
