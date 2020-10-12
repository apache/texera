package texera.common.operators.map

import texera.common.workflow.TexeraOperatorDescriptor

abstract class TexeraMapOpDesc extends TexeraOperatorDescriptor {

  override def texeraOpExec: TexeraMapOpExecConfig

}
