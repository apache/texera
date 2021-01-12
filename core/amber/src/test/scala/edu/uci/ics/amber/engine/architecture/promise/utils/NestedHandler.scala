package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.promise.utils.NestedHandler.{Nested, Pass}
import edu.uci.ics.amber.engine.common.promise.RPCServer.{AsyncRPCCommand, NormalRPCCommand}

object NestedHandler {
  case class Nested(k: Int) extends AsyncRPCCommand[String]

  case class Pass(value: String) extends NormalRPCCommand[String]
}

trait NestedHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandlerAsync[String] {
    case Nested(k) =>
      val retP = Promise[String]()
      send(Pass("Hello"), myID).map { ret: String =>
        send(Pass(" "), myID).map { ret2: String =>
          send(Pass("World!"), myID).map { ret3: String =>
            println(ret + ret2 + ret3)
            retP.setValue(ret + ret2 + ret3)
          }
        }
      }
      retP
  }

  registerHandler[String] {
    case Pass(value) =>
      value
  }
}
