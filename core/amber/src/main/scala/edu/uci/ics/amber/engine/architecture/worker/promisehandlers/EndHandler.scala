package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.gracefulStop
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EndWorkerRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EndWorkerResponse
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.FutureBijection._

import scala.concurrent.Await
import scala.concurrent.duration._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

trait EndHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def endWorker(
      request: EndWorkerRequest,
      ctx: AsyncRPCContext
  ): Future[EndWorkerResponse] = {
    val selection = AmberRuntime.actorSystem.actorSelection(request.actorRefStr)
    val actorRef: ActorRef = Await.result(selection.resolveOne(3.seconds), 3.seconds)
    gracefulStop(actorRef, Duration(5, TimeUnit.SECONDS))
      .asTwitter()
      .map(f => EndWorkerResponse(f))
  }
}
