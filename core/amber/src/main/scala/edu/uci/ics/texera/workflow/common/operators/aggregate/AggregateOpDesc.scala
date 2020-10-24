package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor

abstract class AggregateOpDesc extends OperatorDescriptor {

  override def operatorExecutor: AggregateOpExecConfig[_]

}
