package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Schema, SchemaInfo}

abstract class AggregateOpDesc extends OperatorDescriptor {

  override def operatorExecutor(schemaInfo: SchemaInfo): AggregateOpExecConfig[_]

}
