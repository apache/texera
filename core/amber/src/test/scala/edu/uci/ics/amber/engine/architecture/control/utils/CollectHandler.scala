package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns._

import scala.util.Random

trait CollectHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  override def sendCollect(request: Collect, ctx: AsyncRPCContext): Future[StringResponse] = {
    println(s"start collecting numbers.")
    val p = Future.collect(
      request.workers.indices.map(i =>
        getProxy.sendGenerateNumber(GenerateNumber(), mkContext(request.workers(i)))
      )
    )
    p.map { res =>
      println(s"collected: ${res.mkString(" ")}")
      StringResponse("finished")
    }
  }

  override def sendGenerateNumber(
      request: GenerateNumber,
      ctx: AsyncRPCContext
  ): Future[IntResponse] = {
    IntResponse(Random.nextInt())
  }

}
