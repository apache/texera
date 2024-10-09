package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Future
import edu.uci.ics.amber.engine.common.ambermessage.ChannelMarkerType
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity, ChannelMarkerIdentity}

import scala.language.implicitConversions

class AsyncRPCHandlerInitializer[T](
    ctrlSource: AsyncRPCClient[T],
    ctrlReceiver: AsyncRPCServer
) {
  // register all handlers
  ctrlReceiver.handler = this

  def getProxy:T = ctrlSource.proxy

  def sendChannelMarker(
                         markerId: ChannelMarkerIdentity,
                         markerType: ChannelMarkerType,
                         scope: Set[ChannelIdentity],
                         cmdMapping: Map[ActorVirtualIdentity, ControlInvocation],
                         to: ChannelIdentity
                       ): Unit = {
    ctrlSource.sendChannelMarker(markerId, markerType, scope, cmdMapping, to)
  }

  def createInvocation(methodName: String, payload: Any, to: ActorVirtualIdentity): (ControlInvocation, Future[Any]) =
    ctrlSource.createInvocation(methodName, (payload, ctrlSource.mkContext(to)))

}
