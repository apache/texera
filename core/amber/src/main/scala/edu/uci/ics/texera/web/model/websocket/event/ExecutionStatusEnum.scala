package edu.uci.ics.texera.web.model.websocket.event

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer

@JsonSerialize(`using` = classOf[ExecutionStatusEnumJsonSerializer])
@JsonDeserialize(`using` = classOf[ExecutionStatusEnumJsonDeserializer])
sealed abstract class ExecutionStatusEnum(
    val name: String,
    val code: Byte
) // code indicates the status of the execution in the DB
case object Uninitialized extends ExecutionStatusEnum("Uninitialized", 0)
case object Initializing
    extends ExecutionStatusEnum(
      "Initializing",
      0
    )
case object Running extends ExecutionStatusEnum("Running", 1)
case object Pausing extends ExecutionStatusEnum("Pausing", 1)
case object Paused extends ExecutionStatusEnum("Paused", 2)
case object Resuming extends ExecutionStatusEnum("Resuming", 2)
case object Completed extends ExecutionStatusEnum("Completed", 3)
case object Aborted extends ExecutionStatusEnum("Aborted", 4)

class ExecutionStatusEnumJsonSerializer
    extends StdSerializer[ExecutionStatusEnum](classOf[ExecutionStatusEnum]) {

  override def serialize(
      value: ExecutionStatusEnum,
      gen: JsonGenerator,
      provider: SerializerProvider
  ): Unit = {
    val strValue = value match {
      case Uninitialized => "Uninitialized"
      case Initializing  => "Initializing"
      case Running       => "Running"
      case Pausing       => "Pausing"
      case Paused        => "Paused"
      case Resuming      => "Resuming"
      case Completed     => "Completed"
      case Aborted       => "Aborted"
    }
    gen.writeString(strValue)
  }
}

class ExecutionStatusEnumJsonDeserializer
    extends StdDeserializer[ExecutionStatusEnum](classOf[ExecutionStatusEnum]) {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): ExecutionStatusEnum = {
    p.getText match {
      case "Uninitialized" => Uninitialized
      case "Initializing"  => Initializing
      case "Running"       => Running
      case "Pausing"       => Pausing
      case "Paused"        => Paused
      case "Resuming"      => Resuming
      case "Completed"     => Completed
      case "Aborted"       => Aborted
    }
  }
}
