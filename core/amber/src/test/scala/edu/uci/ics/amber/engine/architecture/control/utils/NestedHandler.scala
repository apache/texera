package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns._

trait NestedHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  override def sendNested(request: Nested, ctx: AsyncRPCContext): Future[StringResponse] = {
    getProxy
      .sendPass(Pass("Hello"), myID)
      .flatMap(ret => getProxy.sendPass(Pass(ret.value + " "), myID))
      .flatMap(ret => getProxy.sendPass(Pass(ret.value + "World!"), myID))
  }

  override def sendPass(request: Pass, ctx: AsyncRPCContext): Future[StringResponse] = {
    StringResponse(request.value)
  }

}
