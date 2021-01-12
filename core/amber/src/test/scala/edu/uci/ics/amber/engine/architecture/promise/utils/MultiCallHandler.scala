package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.promise.utils.ChainHandler.Chain
import edu.uci.ics.amber.engine.architecture.promise.utils.CollectHandler.Collect
import edu.uci.ics.amber.engine.architecture.promise.utils.MultiCallHandler.MultiCall
import edu.uci.ics.amber.engine.architecture.promise.utils.RecursionHandler.Recursion
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.AsyncRPCCommand

object MultiCallHandler{
  case class MultiCall(seq: Seq[ActorVirtualIdentity]) extends AsyncRPCCommand[String]
}



trait MultiCallHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandlerAsync[String] {
    case MultiCall(seq) =>
      val retP = Promise[String]
      send(Chain(seq), myID).map { x: ActorVirtualIdentity =>
        send(Recursion(1), x).map { ret: String =>
          send(Collect(seq.take(3)),myID).map{
            ret =>
              retP.setValue(ret)
          }
        }
      }
      retP
  }

}
