package edu.uci.ics.texera.workflow.common.operators.map

import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor

abstract class TexeraMapOpDesc extends TexeraOperatorDescriptor {

  override def texeraOperatorExecutor: TexeraMapOpExecConfig

}
