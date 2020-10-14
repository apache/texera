package edu.uci.ics.texera.web.model.request

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor

case class ModifyLogicRequest(
    operator: OperatorDescriptor
) extends TexeraWebSocketRequest
