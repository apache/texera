package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor

abstract class TexeraFlatMapOpDesc extends TexeraOperatorDescriptor {

  override def texeraOperatorExecutor: TexeraFlatMapOpExecConfig

}
