package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor

abstract class TexeraAggregateOpDesc extends TexeraOperatorDescriptor {

  override def texeraOperatorExecutor: TexeraAggregateOpExecConfig[_]

}
