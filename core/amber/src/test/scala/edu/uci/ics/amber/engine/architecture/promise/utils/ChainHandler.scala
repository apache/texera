package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.promise.utils.ChainHandler.Chain
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

object ChainHandler {
  case class Chain(nexts: Seq[ActorVirtualIdentity]) extends RPCCommand[ActorVirtualIdentity]
}

trait ChainHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandler { x: Chain =>
    println(s"chained $myID")
    if (x.nexts.isEmpty) {
      Future(myID)
    } else {
      send(Chain(x.nexts.drop(1)), x.nexts.head).map { x =>
        println(s"chain returns from $x")
        x
      }
    }
  }
}
