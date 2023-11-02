package edu.uci.ics.amber.engine.architecture.control.utils

import edu.uci.ics.amber.engine.architecture.control.utils.NestedHandler.{Nested, Pass}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object NestedHandler {
  case class Nested(k: Int) extends ControlCommand[String]

  case class Pass(value: String) extends ControlCommand[String]
}

trait NestedHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  registerHandler { (n: Nested, sender) =>
    send(Pass("Hello"), myID)
      .flatMap(ret => send(Pass(ret + " "), myID))
      .flatMap(ret => send(Pass(ret + "World!"), myID))
  }

  registerHandler { (p: Pass, sender) =>
    p.value
  }
}
