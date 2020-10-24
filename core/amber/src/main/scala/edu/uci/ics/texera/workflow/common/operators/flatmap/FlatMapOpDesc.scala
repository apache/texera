package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}

abstract class FlatMapOpDesc extends OperatorDescriptor {

  override def operatorExecutor: OneToOneOpExecConfig

}
