package edu.uci.ics.texera.workflow.common.operators.mlmodel

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Schema, SchemaInfo}

abstract class MLModelOpDesc extends OperatorDescriptor {

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
//    Schema.newBuilder().build()
    schemas(0)
  }

  override def operatorExecutor(schemaInfo: SchemaInfo): OpExecConfig

}
