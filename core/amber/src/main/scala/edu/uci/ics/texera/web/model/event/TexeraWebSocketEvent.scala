package edu.uci.ics.texera.web.model.event

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[HelloWorldResponse]),
    new Type(value = classOf[WorkflowErrorEvent]),
    new Type(value = classOf[WorkflowStartedEvent]),
    new Type(value = classOf[WorkflowCompletedEvent]),
    new Type(value = classOf[WorkflowStatusUpdateEvent]),
    new Type(value = classOf[WorkflowPausedEvent]),
    new Type(value = classOf[WorkflowResumedEvent]),
    new Type(value = classOf[RecoveryStartedEvent]),
    new Type(value = classOf[BreakpointTriggeredEvent]),
    new Type(value = classOf[ModifyLogicCompletedEvent]),
    new Type(value = classOf[SkipTupleResponseEvent]),
    new Type(value = classOf[OperatorCurrentTuplesUpdateEvent]),
    new Type(value = classOf[PaginatedResultEvent])
  )
)
trait TexeraWebSocketEvent {}
