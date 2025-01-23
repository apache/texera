package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns._

trait ChainHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  override def sendChain(request: Chain, ctx: AsyncRPCContext): Future[StringResponse] = {
    println(s"chained $myID")
    if (request.nexts.isEmpty) {
      Future(StringResponse(myID.name))
    } else {
      getProxy.sendChain(Chain(request.nexts.drop(1)), mkContext(request.nexts.head)).map { x =>
        println(s"chain returns from $x")
        x
      }
    }
  }

}
