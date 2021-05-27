package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.SchemaInfo

abstract class FlatMapOpDesc extends OperatorDescriptor {

  override def operatorExecutor(schemaInfo: SchemaInfo): OneToOneOpExecConfig

}
