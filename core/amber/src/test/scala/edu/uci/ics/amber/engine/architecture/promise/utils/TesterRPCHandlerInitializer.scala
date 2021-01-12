package edu.uci.ics.amber.engine.architecture.promise.utils

import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.{RPCClient, RPCHandlerInitializer, RPCServer}

class TesterRPCHandlerInitializer(
    val myID: ActorVirtualIdentity,
    rpcClient: RPCClient,
    rpcServer: RPCServer
) extends RPCHandlerInitializer(rpcClient, rpcServer)
    with PingPongHandler
    with ChainHandler
    with MultiCallHandler
    with CollectHandler
    with NestedHandler
    with RecursionHandler {}
