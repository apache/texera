package edu.uci.ics.texera.workflow.common.operators.filter

import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Schema, OperatorSchemaInfo}

abstract class FilterOpDesc extends OperatorDescriptor {

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OneToOneOpExecConfig

}
