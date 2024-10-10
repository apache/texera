package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.testcommands.{Recursion, StringResponse}

trait RecursionHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  override def sendRecursion(r: Recursion, ctx: AsyncRPCContext): Future[StringResponse] = {
    if (r.i < 5) {
      println(r.i)
      getProxy.sendRecursion(Recursion(r.i + 1), myID).map { res =>
        println(res)
        r.i.toString
      }
    } else {
      Future(r.i.toString)
    }
  }
}
