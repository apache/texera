package web.model.request

import texera.common.workflow.OperatorDescriptor

case class ModifyLogicRequest(
    operator: OperatorDescriptor
) extends TexeraWsRequest
