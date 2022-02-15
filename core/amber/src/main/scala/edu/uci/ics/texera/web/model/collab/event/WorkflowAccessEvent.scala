package edu.uci.ics.texera.web.model.collab.event

case class WorkflowAccessEvent(workflowReadonly: Boolean) extends CollabWebSocketEvent
