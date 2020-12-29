package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.error.WorkflowRuntimeError

case class WorkflowExecutionErrorEvent(errorMap: Map[String, String]) extends TexeraWebSocketEvent
