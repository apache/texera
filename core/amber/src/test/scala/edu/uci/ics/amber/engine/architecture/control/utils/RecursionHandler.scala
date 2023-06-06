package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.control.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object RecursionHandler {
  case class Recursion(i: Int) extends ControlCommand[String]
}

trait RecursionHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  registerHandler { (r: Recursion, sender) =>
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
