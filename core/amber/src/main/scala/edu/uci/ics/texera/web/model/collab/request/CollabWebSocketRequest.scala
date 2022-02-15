package edu.uci.ics.texera.web.model.collab.request
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[CommandRequest]),
    new Type(value = classOf[AcquireLockRequest]),
    new Type(value = classOf[TryLockRequest]),
    new Type(value = classOf[RestoreVersionRequest]),
    new Type(value = classOf[WIdRequest]),
    new Type(value = classOf[HeartBeatRequest])
  )
)
trait CollabWebSocketRequest {}
