package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerStatistics
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelMarkerPayload, ChannelMarkerType, ControlPayload}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation, TargetedInvocationHandler}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity, ChannelMarkerIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CLIENT

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.collection.mutable
import scala.reflect.ClassTag

/** Motivation of having a separate module to handle control messages as RPCs:
  * In the old design, every control message and its response are handled by
  * message passing. That means developers need to manually send response back
  * and write proper handlers on the sender side.
  * Writing control messages becomes tedious if we use this way.
  *
  * So we want to implement rpc model on top of message passing.
  * rpc (request-response)
  * remote.callFunctionX().then(response => {
  * })
  * user-api: promise
  *
  * goal: request-response model with multiplexing
  * client: initiate request
  * (web browser, actor that invoke control command)
  * server: handle request, return response
  * (web server, actor that handles control command)
  */
object AsyncRPCClient {

  final val IgnoreReply = -1
  final val IgnoreReplyAndDoNotLog = -2

  case class ControlInvocation(reqName: String, payload: (Any, AsyncRPCContext), commandID: Long) extends ControlPayload

  case class ReturnInvocation(originalCommandID: Long, controlReturn: Any) extends ControlPayload

}

class AsyncRPCClient[T:ClassTag](
    outputGateway: NetworkOutputGateway,
    val actorId: ActorVirtualIdentity
) extends AmberLogging {

  private val unfulfilledPromises = mutable.HashMap[Long, Promise[Any]]()
  private var promiseID = 0L
  val proxy: T = createProxy()

  def mkContext(to: ActorVirtualIdentity): AsyncRPCContext = AsyncRPCContext(actorId, to)

  private def createPromise(): (Promise[Any], Long) = {
    promiseID += 1
    val promise = new Promise[Any]()
    unfulfilledPromises(promiseID) = promise
    (promise, promiseID)
  }

  def createInvocation(methodName: String, args:(Any, AsyncRPCContext)):(ControlInvocation, Future[Any]) = {
    val (p, pid) = createPromise()
    (ControlInvocation(methodName, args, pid), p)
  }
  private def createProxy()(implicit ct: ClassTag[T]): T = {
    val handler = new InvocationHandler {

      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
        val (p, pid) = createPromise()
        val context = args(1).asInstanceOf[AsyncRPCContext]
        outputGateway.sendTo(context.receiver, ControlInvocation(method.getName, (args(0), context), pid))
        p
      }
    }

    Proxy.newProxyInstance(
      getClassLoader(ct.runtimeClass),
      Array(ct.runtimeClass),
      handler
    ).asInstanceOf[T]
  }

  // Helper to get the correct class loader
  private def getClassLoader(cls: Class[_]): ClassLoader = {
    Option(cls.getClassLoader).getOrElse(ClassLoader.getSystemClassLoader)
  }


  def sendChannelMarker(
      markerId: ChannelMarkerIdentity,
      markerType: ChannelMarkerType,
      scope: Set[ChannelIdentity],
      cmdMapping: Map[ActorVirtualIdentity, ControlInvocation],
      channelId: ChannelIdentity
  ): Unit = {
    logger.debug(s"send marker: $markerId to $channelId")
    outputGateway.sendTo(
      channelId,
      ChannelMarkerPayload(markerId, markerType, scope, cmdMapping)
    )
  }

  def fulfillPromise(ret: ReturnInvocation): Unit = {
    if (unfulfilledPromises.contains(ret.originalCommandID)) {
      val p = unfulfilledPromises(ret.originalCommandID)
      ret.controlReturn match {
        case error: Throwable =>
          p.setException(error)
        case _ =>
          p.setValue(ret.controlReturn)
      }
      unfulfilledPromises.remove(ret.originalCommandID)
    }
  }

  def logControlReply(ret: ReturnInvocation, channelId: ChannelIdentity): Unit = {
    if (ret.originalCommandID == AsyncRPCClient.IgnoreReplyAndDoNotLog) {
      return
    }
    if (ret.controlReturn != null) {
      if (ret.controlReturn.isInstanceOf[WorkerStatistics]) {
        return
      }
      logger.debug(
        s"receive reply: ${ret.controlReturn.getClass.getSimpleName} from $channelId (controlID: ${ret.originalCommandID})"
      )
      ret.controlReturn match {
        case throwable: Throwable =>
          logger.error(s"received error from $channelId", throwable)
        case _ =>
      }
    } else {
      logger.info(
        s"receive reply: null from $channelId (controlID: ${ret.originalCommandID})"
      )
    }
  }

}
