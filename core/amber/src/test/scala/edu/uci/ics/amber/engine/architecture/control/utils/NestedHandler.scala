package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.control.utils.NestedHandler.{Nested, Pass}
import edu.uci.ics.amber.engine.common.control.ControlMessageReceiver.ControlCommand

object NestedHandler {
  case class Nested(k: Int) extends ControlCommand[String]

  case class Pass(value: String) extends ControlCommand[String]
}

trait NestedHandler {
  this: TesterControlHandlerInitializer =>

  registerHandler { n: Nested =>
    send(Pass("Hello"), myID)
      .flatMap(ret => send(Pass(ret + " "), myID))
      .flatMap(ret => send(Pass(ret + "World!"), myID))
  }

  registerHandler { p: Pass =>
    p.value
  }
}
