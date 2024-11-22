package edu.uci.ics.texera.web.model.websocket.request

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import edu.uci.ics.texera.web.model.websocket.request.python.{
  DebugCommandRequest,
  PythonExpressionEvaluateRequest
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[EditingTimeCompilationRequest]),
    new Type(value = classOf[HeartBeatRequest]),
    new Type(value = classOf[ModifyLogicRequest]),
    new Type(value = classOf[ResultExportRequest]),
    new Type(value = classOf[ResultPaginationRequest]),
    new Type(value = classOf[RetryRequest]),
    new Type(value = classOf[SkipTupleRequest]),
    new Type(value = classOf[WorkflowExecuteRequest]),
    new Type(value = classOf[WorkflowKillRequest]),
    new Type(value = classOf[WorkflowPauseRequest]),
    new Type(value = classOf[WorkflowResumeRequest]),
    new Type(value = classOf[PythonExpressionEvaluateRequest]),
    new Type(value = classOf[DebugCommandRequest]),
    new Type(value = classOf[WorkflowCheckpointRequest])
  )
)
trait TexeraWebSocketRequest {}
