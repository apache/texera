package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor

case class ModifyLogicRequest(operator: OperatorDescriptor) extends TexeraWebSocketRequest
