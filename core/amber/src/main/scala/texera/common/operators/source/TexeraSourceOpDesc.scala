package texera.common.operators.source

import com.google.common.base.Preconditions
import texera.common.operators.TexeraOperatorDescriptor
import texera.common.tuple.schema.Schema

abstract class TexeraSourceOpDesc extends TexeraOperatorDescriptor {

  override def texeraOpExec: TexeraSourceOpExecConfig

  override def transformSchema(schemas: Schema*): Schema = {
    Preconditions.checkArgument(schemas.isEmpty)
    sourceSchema()
  }

  def sourceSchema(): Schema

}
