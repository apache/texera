package web.model.request

import texera.common.operators.TexeraOperatorDescriptor

case class ModifyLogicRequest(
    operator: TexeraOperatorDescriptor
) extends TexeraWsRequest
