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

  registerHandler {
    case Chain(nexts) =>
      println(s"chained $myID")
      if (nexts.isEmpty) {
        Future(myID)
      } else {
        send(Chain(nexts.drop(1)), nexts.head).map { x =>
          println(s"chain returns from $x")
          x
        }
      }
  }
}
