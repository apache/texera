package edu.uci.ics.amber.operator.source

import com.google.common.base.Preconditions
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.operator.LogicalOp

abstract class SourceOperatorDescriptor extends LogicalOp {

  def sourceSchema(): Schema

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.isEmpty)
    sourceSchema()
  }
}
