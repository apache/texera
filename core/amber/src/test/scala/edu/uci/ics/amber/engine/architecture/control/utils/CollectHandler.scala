package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.control.utils.CollectHandler.{Collect, GenerateNumber}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

import scala.util.Random

object CollectHandler {
  case class Collect(workers: Seq[ActorVirtualIdentity]) extends ControlCommand[String]
  case class GenerateNumber() extends ControlCommand[Int]
}

trait CollectHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  registerHandler { c: Collect =>
    println(s"start collecting numbers.")
    val p = Future.collect(c.workers.indices.map(i => send(GenerateNumber(), c.workers(i))))
    p.map { res =>
      println(s"collected: ${res.mkString(" ")}")
      "finished"
    }
  }

  registerHandler { g: GenerateNumber =>
    Random.nextInt()
  }
}
