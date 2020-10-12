package texera.common.operators.flatmap

import texera.common.workflow.TexeraOperatorDescriptor

abstract class TexeraFlatMapOpDesc extends TexeraOperatorDescriptor {

  override def texeraOpExec: TexeraFlatMapOpExecConfig

}
