package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.promise.utils.PingPongHandler.{Ping, Pong}
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

object PingPongHandler {
  case class Ping(i: Int, end: Int, myID: ActorVirtualIdentity) extends RPCCommand[Int]

  case class Pong(i: Int, end: Int, myID: ActorVirtualIdentity) extends RPCCommand[Int]
}

trait PingPongHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandler {
    case Ping(i, e, to) =>
      println(s"$i ping")
      if (i < e) {
        send(Pong(i + 1, e, myID), to).map { ret: Int =>
          println(s"$i ping replied with value $ret!")
          ret
        }
      } else {
        Future(i)
      }
    case Pong(i, e, to) =>
      println(s"$i pong")
      if (i < e) {
        send(Ping(i + 1, e, myID), to).map { ret: Int =>
          println(s"$i pong replied with value $ret!")
          ret
        }
      } else {
        Future(i)
      }
  }

}
