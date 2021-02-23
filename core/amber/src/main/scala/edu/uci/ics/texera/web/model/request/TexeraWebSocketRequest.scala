package edu.uci.ics.texera.web.model.request

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[HelloWorldRequest]),
    new Type(value = classOf[ExecuteWorkflowRequest]),
    new Type(value = classOf[PauseWorkflowRequest]),
    new Type(value = classOf[ResumeWorkflowRequest]),
    new Type(value = classOf[KillWorkflowRequest]),
    new Type(value = classOf[ModifyLogicRequest]),
    new Type(value = classOf[AddBreakpointRequest]),
    new Type(value = classOf[RemoveBreakpointRequest]),
    new Type(value = classOf[SkipTupleRequest]),
    new Type(value = classOf[ResultPaginationRequest])
  )
)
trait TexeraWebSocketRequest {}
