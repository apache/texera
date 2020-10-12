package web.model.request

import texera.common.workflow.TexeraOperatorDescriptor

case class ModifyLogicRequest(
    operator: TexeraOperatorDescriptor
) extends TexeraWsRequest
