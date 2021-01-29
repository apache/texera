package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.control.utils.PingPongHandler.{Ping, Pong}
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object PingPongHandler {
  case class Ping(i: Int, end: Int, to: ActorVirtualIdentity) extends ControlCommand[Int]

  case class Pong(i: Int, end: Int, to: ActorVirtualIdentity) extends ControlCommand[Int]
}

trait PingPongHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  registerHandler { (ping: Ping, sender) =>
    println(s"${ping.i} ping")
    if (ping.i < ping.end) {
      send(Pong(ping.i + 1, ping.end, myID), ping.to).map { ret: Int =>
        println(s"${ping.i} ping replied with value $ret!")
        ret
      }
    } else {
      Future(ping.i)
    }
  }

  registerHandler { (pong: Pong, sender) =>
    println(s"${pong.i} pong")
    if (pong.i < pong.end) {
      send(Ping(pong.i + 1, pong.end, myID), pong.to).map { ret: Int =>
        println(s"${pong.i} pong replied with value $ret!")
        ret
      }
    } else {
      Future(pong.i)
    }
  }

}
