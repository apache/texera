package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ClientEvent
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceFs2Grpc
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns._
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceFs2Grpc
import edu.uci.ics.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  ChannelMarkerIdentity
}

import scala.language.implicitConversions

class AsyncRPCHandlerInitializer(
    ctrlSource: AsyncRPCClient,
    ctrlReceiver: AsyncRPCServer
) {
  implicit def returnAsFuture[R](ret: R): Future[R] = Future[R](ret)

  implicit def actorIdAsContext(to: ActorVirtualIdentity): AsyncRPCContext = mkContext(to)

  implicit def stringToResponse(s: String): StringResponse = StringResponse(s)

  implicit def intToResponse(i: Int): IntResponse = IntResponse(i)

  // register all handlers
  ctrlReceiver.handler = this

  def controllerInterface: ControllerServiceFs2Grpc[Future, AsyncRPCContext] =
    ctrlSource.controllerInterface

  def workerInterface: WorkerServiceFs2Grpc[Future, AsyncRPCContext] = ctrlSource.workerInterface

  def mkContext(to: ActorVirtualIdentity): AsyncRPCContext = ctrlSource.mkContext(to)

  def sendChannelMarker(
      markerId: ChannelMarkerIdentity,
      markerType: ChannelMarkerType,
      scope: Set[ChannelIdentity],
      cmdMapping: Map[String, ControlInvocation],
      to: ChannelIdentity
  ): Unit = {
    ctrlSource.sendChannelMarker(markerId, markerType, scope, cmdMapping, to)
  }

  def sendToClient(clientEvent: ClientEvent): Unit = {
    ctrlSource.sendToClient(clientEvent)
  }

  def createInvocation(
      methodName: String,
      payload: ControlRequest,
      to: ActorVirtualIdentity
  ): (ControlInvocation, Future[ControlReturn]) =
    ctrlSource.createInvocation(methodName, payload, ctrlSource.mkContext(to))

}
