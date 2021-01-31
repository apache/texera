package edu.uci.ics.amber.engine.architecture.principal

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import edu.uci.ics.amber.engine.architecture.principal.OperatorState.OperatorStateJsonSerializer

@JsonSerialize(using = classOf[OperatorStateJsonSerializer])
sealed abstract class OperatorState extends Serializable

object OperatorState {
  case object Uninitialized extends OperatorState
  case object Ready extends OperatorState
  case object Running extends OperatorState
  case object Paused extends OperatorState
  case object Pausing extends OperatorState
  case object Completed extends OperatorState
  case object Recovering extends OperatorState
  case object Unknown extends OperatorState

  class OperatorStateJsonSerializer extends StdSerializer[OperatorState](classOf[OperatorState]) {

    override def serialize(
        value: OperatorState,
        gen: JsonGenerator,
        provider: SerializerProvider
    ): Unit = {
      val strValue = value match {
        case Uninitialized => "Uninitialized"
        case Ready         => "Ready"
        case Running       => "Running"
        case Paused        => "Paused"
        case Pausing       => "Pausing"
        case Completed     => "Completed"
        case Recovering    => "Recovering"
        case Unknown       => "Unknown"
      }
      gen.writeString(strValue)
    }
  }

}
