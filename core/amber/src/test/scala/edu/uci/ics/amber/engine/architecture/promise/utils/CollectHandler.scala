package edu.uci.ics.amber.engine.architecture.promise.utils


import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.promise.utils.CollectHandler.{Collect, GenerateNumber}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.{AsyncRPCCommand, NormalRPCCommand}

import scala.util.Random

object CollectHandler{
  case class Collect(workers: Seq[ActorVirtualIdentity]) extends AsyncRPCCommand[String]
  case class GenerateNumber() extends NormalRPCCommand[Int]
}


trait CollectHandler {
  this: TesterRPCHandlerInitializer =>

  registerHandlerAsync[String] {
    case Collect(workers) =>
      println(s"start collecting numbers.")
      val tasks = workers.indices.map(i => (GenerateNumber(), workers(i)))
      val p = Future.collect(tasks.map(i => send(i._1,i._2)))
      val retP = Promise[String]()
      p.map { res =>
        println(s"collected: ${res.mkString(" ")}")
        retP.setValue("finished")
      }
    retP
  }

  registerHandler[Int]{
    case GenerateNumber() =>
      Random.nextInt()
  }
}
