package texera.common.workflow.common

import Engine.Operators.Common.Map.MapOpExecConfig
import texera.common.workflow.OperatorDescriptor

abstract class MapOpDesc extends OperatorDescriptor {

  override def amberOperator: MapOpExecConfig

}
