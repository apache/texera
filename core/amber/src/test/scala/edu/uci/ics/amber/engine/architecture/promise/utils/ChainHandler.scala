package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.promise.utils.ChainHandler.Chain
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.{AsyncRPCCommand, RPCCommand}

object ChainHandler{
  case class Chain(nexts: Seq[ActorVirtualIdentity])
    extends AsyncRPCCommand[ActorVirtualIdentity]
}



trait ChainHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandlerAsync[ActorVirtualIdentity] {
    case Chain(nexts) =>
      println(s"chained $myID")
      val retP = Promise[ActorVirtualIdentity]()
      if (nexts.isEmpty) {
        retP.setValue(myID)
      } else {
        send(Chain(nexts.drop(1)), nexts.head).map { x =>
          println(s"chain returns from $x")
          retP.setValue(x)
        }
      }
    retP
  }
}
