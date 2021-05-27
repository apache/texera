package edu.uci.ics.texera.workflow.common.operators.map

import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.SchemaInfo

abstract class MapOpDesc extends OperatorDescriptor {

  override def operatorExecutor(schemaInfo: SchemaInfo): OneToOneOpExecConfig

}
