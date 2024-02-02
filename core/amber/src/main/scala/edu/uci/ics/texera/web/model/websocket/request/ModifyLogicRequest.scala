package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.texera.workflow.common.operators.LogicalOp

case class ModifyLogicRequest(operator: LogicalOp) extends TexeraWebSocketRequest
