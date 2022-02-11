package edu.uci.ics.texera.web.model.collab.event

case class WorkflowAccessEvent(isWorkflowReadonly: Boolean) extends CollabWebSocketEvent
