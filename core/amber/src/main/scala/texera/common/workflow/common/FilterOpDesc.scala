package texera.common.workflow.common

import Engine.Common.tuple.texera.schema.Schema
import com.google.common.base.Preconditions
import texera.common.workflow.OperatorDescriptor

abstract class FilterOpDesc extends OperatorDescriptor {

  override def transformSchema(schemas: Schema*): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }

}
