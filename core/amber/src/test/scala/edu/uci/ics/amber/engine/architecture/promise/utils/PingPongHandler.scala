package edu.uci.ics.amber.engine.architecture.promise.utils

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.promise.utils.PingPongHandler.{Ping, Pong}
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.{AsyncRPCCommand, RPCCommand}

object PingPongHandler {
  case class Ping(i: Int, end: Int, myID: ActorVirtualIdentity) extends AsyncRPCCommand[Int]

  case class Pong(i: Int, end: Int, myID: ActorVirtualIdentity) extends AsyncRPCCommand[Int]
}

trait PingPongHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandlerAsync[Int] {
    case Ping(i, e, to) =>
      println(s"$i ping")
      val retP = Promise[Int]()
      if (i < e) {
        send(Pong(i + 1, e, myID), to).map { ret: Int =>
          println(s"$i ping replied with value $ret!")
          retP.setValue(ret)
        }
      } else {
        retP.setValue(i)
      }
      retP
    case Pong(i, e, to) =>
      println(s"$i pong")
      val retP = Promise[Int]()
      if (i < e) {
        send(Ping(i + 1, e, myID), to).map { ret: Int =>
          println(s"$i pong replied with value $ret!")
          retP.setValue(ret)
        }
      } else {
        retP.setValue(i)
      }
      retP
  }

}
