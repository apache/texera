package edu.uci.ics.texera.web.model.websocket.event

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import edu.uci.ics.texera.web.model.websocket.event.python.PythonPrintTriggeredEvent
import edu.uci.ics.texera.web.model.websocket.response.python.PythonExpressionEvaluateResponse
import edu.uci.ics.texera.web.model.websocket.response.{HeartBeatResponse, HelloWorldResponse}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[HelloWorldResponse]),
    new Type(value = classOf[HeartBeatResponse]),
    new Type(value = classOf[WorkflowErrorEvent]),
    new Type(value = classOf[WorkflowStartedEvent]),
    new Type(value = classOf[WorkflowCompletedEvent]),
    new Type(value = classOf[WebWorkflowStatusUpdateEvent]),
    new Type(value = classOf[WorkflowPausedEvent]),
    new Type(value = classOf[WorkflowResumedEvent]),
    new Type(value = classOf[RecoveryStartedEvent]),
    new Type(value = classOf[BreakpointTriggeredEvent]),
    new Type(value = classOf[PythonPrintTriggeredEvent]),
    new Type(value = classOf[OperatorCurrentTuplesUpdateEvent]),
    new Type(value = classOf[CacheStatusUpdateEvent]),
    new Type(value = classOf[PaginatedResultEvent]),
    new Type(value = classOf[PythonExpressionEvaluateResponse])
  )
)
trait TexeraWebSocketEvent {}
