package edu.uci.ics.texera.web.model.websocket.event

case class WorkerAssignmentUpdateEvent(operatorId: String, workerIds: Seq[String])
    extends TexeraWebSocketEvent
