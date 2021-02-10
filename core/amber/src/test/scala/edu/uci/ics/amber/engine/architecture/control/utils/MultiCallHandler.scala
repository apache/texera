package edu.uci.ics.amber.engine.architecture.control.utils

import edu.uci.ics.amber.engine.architecture.control.utils.ChainHandler.Chain
import edu.uci.ics.amber.engine.architecture.control.utils.CollectHandler.Collect
import edu.uci.ics.amber.engine.architecture.control.utils.MultiCallHandler.MultiCall
import edu.uci.ics.amber.engine.architecture.control.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object MultiCallHandler {
  case class MultiCall(seq: Seq[ActorVirtualIdentity]) extends ControlCommand[String]
}

trait MultiCallHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  registerHandler { (m: MultiCall, sender) =>
    send(Chain(m.seq), myID)
      .flatMap(x => send(Recursion(1), x))
      .flatMap(ret => send(Collect(m.seq.take(3)), myID))
  }

}
