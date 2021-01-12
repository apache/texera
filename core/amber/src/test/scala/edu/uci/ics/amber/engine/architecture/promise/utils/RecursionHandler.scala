package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.promise.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.common.promise.RPCServer.AsyncRPCCommand


object RecursionHandler {
  case class Recursion(i: Int) extends AsyncRPCCommand[String]
}

trait RecursionHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandlerAsync[String] {
    case Recursion(i) =>
      val retP = Promise[String]
    if (i < 5) {
      println(i)
      send(Recursion(i + 1), myID).map { res =>
        println(res)
        retP.setValue(i.toString)
      }
    } else {
      retP.setValue(i.toString)
    }
    retP
  }

}
