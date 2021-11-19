package edu.uci.ics.texera.web.model.websocket.event

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer

@JsonSerialize(`using` = classOf[ExecutionStatusEnumJsonSerializer])
@JsonDeserialize(`using` = classOf[ExecutionStatusEnumJsonDeserializer])
sealed trait ExecutionStatusEnum
case object Uninitialized extends ExecutionStatusEnum
case object Initializing extends ExecutionStatusEnum
case object Running extends ExecutionStatusEnum
case object Pausing extends ExecutionStatusEnum
case object Paused extends ExecutionStatusEnum
case object Resuming extends ExecutionStatusEnum
case object Completed extends ExecutionStatusEnum
case object Aborted extends ExecutionStatusEnum

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
