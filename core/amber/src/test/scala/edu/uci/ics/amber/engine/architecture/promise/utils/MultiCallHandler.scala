package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.promise.utils.ChainHandler.Chain
import edu.uci.ics.amber.engine.architecture.promise.utils.CollectHandler.Collect
import edu.uci.ics.amber.engine.architecture.promise.utils.MultiCallHandler.MultiCall
import edu.uci.ics.amber.engine.architecture.promise.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

object MultiCallHandler {
  case class MultiCall(seq: Seq[ActorVirtualIdentity]) extends RPCCommand[String]
}

trait MultiCallHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandler {
    case MultiCall(seq) =>
      send(Chain(seq), myID)
        .flatMap(x => send(Recursion(1), x))
        .flatMap(ret => send(Collect(seq.take(3)), myID))
  }

}
