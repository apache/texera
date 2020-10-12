package texera.common.operators.flatmap

import texera.common.operators.TexeraOperatorDescriptor

abstract class TexeraFlatMapOpDesc extends TexeraOperatorDescriptor {

  override def texeraOpExec: TexeraFlatMapOpExecConfig

}
