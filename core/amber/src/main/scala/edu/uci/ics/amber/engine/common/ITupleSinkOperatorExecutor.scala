package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode

trait ITupleSinkOperatorExecutor extends IOperatorExecutor {

  def getResultTuples(): List[ITuple]

  def getOutputMode(): IncrementalOutputMode

}
