package texera.common.operators.map

import texera.common.operators.TexeraOperatorDescriptor

abstract class TexeraMapOpDesc extends TexeraOperatorDescriptor {

  override def texeraOperatorExecutor: TexeraMapOpExecConfig

}
