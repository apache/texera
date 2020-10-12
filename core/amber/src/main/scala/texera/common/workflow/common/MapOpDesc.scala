package texera.common.workflow.common

import texera.operators.Common.Map.MapOpExecConfig
import texera.common.workflow.TexeraOperatorDescriptor

abstract class MapOpDesc extends TexeraOperatorDescriptor {

  override def amberOperator: MapOpExecConfig

}
