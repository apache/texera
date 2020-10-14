package edu.uci.ics.texera.workflow.common.operators.map

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor

abstract class MapOpDesc extends OperatorDescriptor {

  override def operatorExecutor: MapOpExecConfig

}
