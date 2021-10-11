package edu.uci.ics.texera.web.model.event

import edu.uci.ics.texera.web.resource.execution.WorkflowResultService.WebResultUpdate

case class WebResultUpdateEvent(updates: Map[String, WebResultUpdate])
    extends TexeraWebSocketEvent
    with FrameSynchronization
