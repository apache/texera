package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.NewOpExecConfig.NewOpExecConfig
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

abstract class AggregateOpDesc extends OperatorDescriptor {

  override def newOperatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): NewOpExecConfig = {
    throw new UnsupportedOperationException("multi-layer op should use operatorExecutorMultiLayer")
  }

}
