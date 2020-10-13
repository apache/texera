package texera.common.operators.aggregate

import texera.common.operators.TexeraOperatorDescriptor

abstract class TexeraAggregateOpDesc extends TexeraOperatorDescriptor {

  override def texeraOperatorExecutor: TexeraAggregateOpExecConfig[_]

}
