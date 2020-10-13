package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.Tuple


trait TupleSinkOperatorExecutor extends OperatorExecutor {

  def getResultTuples(): Array[Tuple]

}
