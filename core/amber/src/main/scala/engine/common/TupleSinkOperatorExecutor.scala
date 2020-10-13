package engine.common

import engine.common.tuple.Tuple


trait TupleSinkOperatorExecutor extends OperatorExecutor {

  def getResultTuples(): Array[Tuple]

}
