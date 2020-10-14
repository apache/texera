package edu.uci.ics.texera.workflow.common.operators.source

import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

abstract class SourceOperatorDescriptor extends OperatorDescriptor {

  override def operatorExecutor: SourceOpExecConfig

  override def getOutputSchema(schemas: Schema*): Schema = {
    Preconditions.checkArgument(schemas.isEmpty)
    sourceSchema()
  }

  def sourceSchema(): Schema

}
