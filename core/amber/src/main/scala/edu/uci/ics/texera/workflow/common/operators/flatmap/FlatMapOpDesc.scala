package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor

abstract class FlatMapOpDesc extends OperatorDescriptor {

  override def operatorExecutor: FlatMapOpExecConfig

}
