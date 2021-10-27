package edu.uci.ics.texera.web.model.websocket.request

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import edu.uci.ics.texera.web.model.websocket.request.python.PythonExpressionEvaluateRequest

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[RegisterWIdRequest]),
    new Type(value = classOf[AddBreakpointRequest]),
    new Type(value = classOf[CacheStatusUpdateRequest]),
    new Type(value = classOf[HeartBeatRequest]),
    new Type(value = classOf[ModifyLogicRequest]),
    new Type(value = classOf[RemoveBreakpointRequest]),
    new Type(value = classOf[ResultExportRequest]),
    new Type(value = classOf[ResultPaginationRequest]),
    new Type(value = classOf[RetryRequest]),
    new Type(value = classOf[SkipTupleRequest]),
    new Type(value = classOf[WorkflowExecuteRequest]),
    new Type(value = classOf[WorkflowKillRequest]),
    new Type(value = classOf[WorkflowPauseRequest]),
    new Type(value = classOf[WorkflowResumeRequest]),
    new Type(value = classOf[PythonExpressionEvaluateRequest])
  )
)
trait TexeraWebSocketRequest {}
