package texera.common.operators.filter

import Engine.Common.tuple.texera.schema.Schema
import com.google.common.base.Preconditions
import texera.common.operators.TexeraOperatorDescriptor

abstract class TexeraFilterOpDesc extends TexeraOperatorDescriptor {

  override def transformSchema(schemas: Schema*): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }

  override def texeraOpExec: TexeraFilterOpExecConfig

}
