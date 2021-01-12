package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.promise.utils.NestedHandler.{Nested, Pass}
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

object NestedHandler {
  case class Nested(k: Int) extends RPCCommand[String]

  case class Pass(value: String) extends RPCCommand[String]
}

trait NestedHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandler {
    case Nested(k) =>
      send(Pass("Hello"), myID)
        .flatMap(ret => send(Pass(ret + " "), myID))
        .flatMap(ret => send(Pass(ret + "World!"), myID))
  }

  registerHandler {
    case Pass(value) =>
      value
  }
}
