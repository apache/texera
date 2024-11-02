package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns._
trait PingPongHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  override def sendPing(ping: Ping, ctx: AsyncRPCContext): Future[IntResponse] = {
    println(s"${ping.i} ping")
    if (ping.i < ping.end) {
      getProxy.sendPong(Pong(ping.i + 1, ping.end, myID), ping.to).map { ret: IntResponse =>
        println(s"${ping.i} ping replied with value ${ret.value}!")
        ret
      }
    } else {
      Future(ping.i)
    }
  }

  override def sendPong(pong: Pong, ctx: AsyncRPCContext): Future[IntResponse] = {
    println(s"${pong.i} pong")
    if (pong.i < pong.end) {
      getProxy.sendPing(Ping(pong.i + 1, pong.end, myID), pong.to).map { ret: IntResponse =>
        println(s"${pong.i} pong replied with value ${ret.value}!")
        ret
      }
    } else {
      Future(pong.i)
    }
  }

}
