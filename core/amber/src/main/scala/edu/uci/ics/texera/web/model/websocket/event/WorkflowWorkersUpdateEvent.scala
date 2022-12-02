package edu.uci.ics.texera.web.model.websocket.event

case class WorkflowWorkersUpdateEvent(operatorId: String, workerIds: Seq[String])
    extends TexeraWebSocketEvent
