package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.testcommands.Chain
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

trait ChainHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  override def sendChain(request: Chain, ctx: AsyncRPCContext): Future[ActorVirtualIdentity] = {
    println(s"chained $myID")
    if (request.nexts.isEmpty) {
      Future(myID)
    } else {
      getProxy.sendChain(Chain(request.nexts.drop(1)), mkContext(request.nexts.head)).map { x =>
        println(s"chain returns from $x")
        x
      }
    }
  }

}
