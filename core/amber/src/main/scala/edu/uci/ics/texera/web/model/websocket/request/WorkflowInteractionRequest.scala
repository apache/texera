package edu.uci.ics.texera.web.model.websocket.request

case class WorkflowInteractionRequest() extends TexeraWebSocketRequest()

case class WorkflowStepRequest(stepType:String, targetOp:String) extends TexeraWebSocketRequest()
