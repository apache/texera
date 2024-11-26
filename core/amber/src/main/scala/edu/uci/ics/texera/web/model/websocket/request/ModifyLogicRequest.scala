package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.amber.operator.LogicalOp

case class ModifyLogicRequest(operator: LogicalOp) extends TexeraWebSocketRequest
