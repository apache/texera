package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.ITuple

trait ITupleSinkOperatorExecutor extends IOperatorExecutor {

  def getResultTuples(): List[ITuple]

}
