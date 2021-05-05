package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

abstract class FlatMapOpDesc extends OperatorDescriptor {

  override def operatorExecutor(inputSchemas: Array[Schema], outputSchema: Schema): OneToOneOpExecConfig

}
