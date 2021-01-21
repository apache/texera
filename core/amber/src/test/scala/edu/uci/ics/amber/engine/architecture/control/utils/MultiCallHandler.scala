package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.control.utils.ChainHandler.Chain
import edu.uci.ics.amber.engine.architecture.control.utils.CollectHandler.Collect
import edu.uci.ics.amber.engine.architecture.control.utils.MultiCallHandler.MultiCall
import edu.uci.ics.amber.engine.architecture.control.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object MultiCallHandler {
  case class MultiCall(seq: Seq[ActorVirtualIdentity]) extends ControlCommand[String]
}

trait MultiCallHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  registerHandler { m: MultiCall =>
    send(Chain(m.seq), myID)
      .flatMap(x => send(Recursion(1), x))
      .flatMap(ret => send(Collect(m.seq.take(3)), myID))
  }

}
