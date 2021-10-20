package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.service.JobRuntimeService.ExecutionStatusEnum

case class WorkflowStatusEvent(status:ExecutionStatusEnum) extends TexeraWebSocketEvent
