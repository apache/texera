package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.promise.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

object RecursionHandler {
  case class Recursion(i: Int) extends RPCCommand[String]
}

trait RecursionHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandler { r: Recursion =>
    if (r.i < 5) {
      println(r.i)
      send(Recursion(r.i + 1), myID).map { res =>
        println(res)
        r.i.toString
      }
    } else {
      Future(r.i.toString)
    }
  }
}
