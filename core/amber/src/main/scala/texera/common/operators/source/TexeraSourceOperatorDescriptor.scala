package texera.common.operators.source

import com.google.common.base.Preconditions
import texera.common.operators.TexeraOperatorDescriptor
import texera.common.tuple.schema.Schema

abstract class TexeraSourceOperatorDescriptor extends TexeraOperatorDescriptor {

  override def texeraOperatorExecutor: TexeraSourceOpExecConfig

  override def transformSchema(schemas: Schema*): Schema = {
    Preconditions.checkArgument(schemas.isEmpty)
    sourceSchema()
  }

  def sourceSchema(): Schema

}
