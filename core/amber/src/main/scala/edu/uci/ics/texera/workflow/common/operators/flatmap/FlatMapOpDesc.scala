package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

abstract class FlatMapOpDesc extends OperatorDescriptor {

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OneToOneOpExecConfig

}
