package web.model.request

import texera.common.workflow.TexeraOperator

case class ModifyLogicRequest(
    operator: TexeraOperator
) extends TexeraWsRequest
