package Engine.Common

import Engine.Common.tuple.Tuple


trait TupleSinkOperatorExecutor extends OperatorExecutor {

  def getResultTuples(): Array[Tuple]

}
