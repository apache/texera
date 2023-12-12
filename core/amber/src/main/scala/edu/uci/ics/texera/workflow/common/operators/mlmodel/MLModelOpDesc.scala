package edu.uci.ics.texera.workflow.common.operators.mlmodel

import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

abstract class MLModelOpDesc extends LogicalOp {

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
//    Schema.newBuilder().build()
    schemas(0)
  }

}
