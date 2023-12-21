package edu.uci.ics.texera.web.model.websocket.event

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import edu.uci.ics.texera.web.model.websocket.event.python.ConsoleUpdateEvent
import edu.uci.ics.texera.web.model.websocket.response.python.PythonExpressionEvaluateResponse
import edu.uci.ics.texera.web.model.websocket.response.{
  HeartBeatResponse,
  ModifyLogicResponse,
  RegisterWorkflowIdResponse
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[RegisterWorkflowIdResponse]),
    new Type(value = classOf[HeartBeatResponse]),
    new Type(value = classOf[WorkflowErrorEvent]),
    new Type(value = classOf[WorkflowStateEvent]),
    new Type(value = classOf[OperatorStatisticsUpdateEvent]),
    new Type(value = classOf[WebResultUpdateEvent]),
    new Type(value = classOf[ReportFaultedTupleEvent]),
    new Type(value = classOf[ConsoleUpdateEvent]),
    new Type(value = classOf[OperatorCurrentTuplesUpdateEvent]),
    new Type(value = classOf[CacheStatusUpdateEvent]),
    new Type(value = classOf[PaginatedResultEvent]),
    new Type(value = classOf[PythonExpressionEvaluateResponse]),
    new Type(value = classOf[WorkerAssignmentUpdateEvent]),
    new Type(value = classOf[ModifyLogicResponse])
  )
)
trait TexeraWebSocketEvent {}
