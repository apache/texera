package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns._
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

trait MultiCallHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  override def sendMultiCall(request: MultiCall, ctx: AsyncRPCContext): Future[StringResponse] = {
    getProxy
      .sendChain(Chain(request.seq), myID)
      .flatMap(x => getProxy.sendRecursion(Recursion(1), mkContext(ActorVirtualIdentity(x.value))))
      .flatMap(ret => getProxy.sendCollect(Collect(request.seq.take(3)), myID))
  }

}
