package edu.uci.ics.amber.engine.architecture.control.utils

import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCClient,
  AsyncRPCHandlerInitializer,
  AsyncRPCServer
}

class TesterAsyncRPCHandlerInitializer(
                                       val myID: ActorVirtualIdentity,
                                       source: AsyncRPCClient,
                                       receiver: AsyncRPCServer
) extends AsyncRPCHandlerInitializer(source, receiver)
    with PingPongHandler
    with ChainHandler
    with MultiCallHandler
    with CollectHandler
    with NestedHandler
    with RecursionHandler {}
