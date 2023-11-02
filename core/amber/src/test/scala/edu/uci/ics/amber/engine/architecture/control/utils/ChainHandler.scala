package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.control.utils.ChainHandler.Chain
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object ChainHandler {
  case class Chain(nexts: Seq[ActorVirtualIdentity]) extends ControlCommand[ActorVirtualIdentity]
}

trait ChainHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  registerHandler { (x: Chain, sender) =>
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
