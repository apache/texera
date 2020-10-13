package texera.common.operators.filter

import com.google.common.base.Preconditions
import texera.common.operators.TexeraOperatorDescriptor
import texera.common.tuple.schema.Schema

abstract class TexeraFilterOpDesc extends TexeraOperatorDescriptor {

  override def transformSchema(schemas: Schema*): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }

  override def texeraOperatorExecutor: TexeraFilterOpExecConfig

}
