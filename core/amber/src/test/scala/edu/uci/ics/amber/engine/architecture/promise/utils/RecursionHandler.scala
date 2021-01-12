package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.promise.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

object RecursionHandler {
  case class Recursion(i: Int) extends RPCCommand[String]
}

trait RecursionHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandler {
    case Recursion(i) =>
      if (i < 5) {
        println(i)
        send(Recursion(i + 1), myID).map { res =>
          println(res)
          i.toString
        }
      } else {
        Future(i.toString)
      }
  }

}
