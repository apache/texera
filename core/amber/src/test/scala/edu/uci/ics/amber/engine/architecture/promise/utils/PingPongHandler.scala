package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.promise.utils.PingPongHandler.{Ping, Pong}
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

object PingPongHandler {
  case class Ping(i: Int, end: Int, to: ActorVirtualIdentity) extends RPCCommand[Int]

  case class Pong(i: Int, end: Int, to: ActorVirtualIdentity) extends RPCCommand[Int]
}

trait PingPongHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandler {
    ping:Ping =>
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


  registerHandler{
    pong:Pong =>
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
