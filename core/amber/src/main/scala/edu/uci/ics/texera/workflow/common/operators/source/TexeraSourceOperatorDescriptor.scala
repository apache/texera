package edu.uci.ics.texera.workflow.common.operators.source

import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

abstract class TexeraSourceOperatorDescriptor extends TexeraOperatorDescriptor {

  override def texeraOperatorExecutor: TexeraSourceOpExecConfig

  override def transformSchema(schemas: Schema*): Schema = {
    Preconditions.checkArgument(schemas.isEmpty)
    sourceSchema()
  }

  def sourceSchema(): Schema

}
