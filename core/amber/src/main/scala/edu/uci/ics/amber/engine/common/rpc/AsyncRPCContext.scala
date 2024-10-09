package edu.uci.ics.amber.engine.common.rpc

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

case class AsyncRPCContext(sender:ActorVirtualIdentity, receiver:ActorVirtualIdentity)
