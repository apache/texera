package Engine.Common

import Engine.Common.tuple.Tuple


trait TupleSink extends OperatorExecutor {

  def getResultTuples(): Array[Tuple]

}
