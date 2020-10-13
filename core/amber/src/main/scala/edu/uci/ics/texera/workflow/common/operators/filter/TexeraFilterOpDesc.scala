package edu.uci.ics.texera.workflow.common.operators.filter

import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

abstract class TexeraFilterOpDesc extends TexeraOperatorDescriptor {

  override def transformSchema(schemas: Schema*): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }

  override def texeraOperatorExecutor: TexeraFilterOpExecConfig

}
