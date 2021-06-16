package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

abstract class AggregateOpDesc extends OperatorDescriptor {

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): AggregateOpExecConfig[_]

}
