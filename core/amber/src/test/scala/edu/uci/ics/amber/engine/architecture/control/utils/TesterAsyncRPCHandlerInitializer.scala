package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AsyncRPCContext
import edu.uci.ics.amber.engine.architecture.rpc.testerservice.RPCTesterFs2Grpc
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class TesterAsyncRPCHandlerInitializer(
    val myID: ActorVirtualIdentity,
    source: AsyncRPCClient[RPCTesterFs2Grpc[Future, AsyncRPCContext]],
    receiver: AsyncRPCServer
) extends AsyncRPCHandlerInitializer(source, receiver)
  with RPCTesterFs2Grpc[Future, AsyncRPCContext]
    with PingPongHandler
    with ChainHandler
    with MultiCallHandler
    with CollectHandler
    with NestedHandler
    with RecursionHandler
    with ErrorHandler {
}
