package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.controller.ClientEvent
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  ChannelMarkerIdentity
}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CLIENT
import io.grpc.MethodDescriptor
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ChannelMarkerPayload,
  ChannelMarkerType,
  ControlInvocation,
  ControlRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceFs2Grpc
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{
  ControlError,
  ControlReturn,
  ReturnInvocation,
  WorkerMetricsResponse
}
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceFs2Grpc
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.createProxy
import edu.uci.ics.amber.error.ErrorUtils.reconstructThrowable

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

  object ControlInvocation {
    def apply(
        method: MethodDescriptor[_, _],
        payload: ControlRequest,
        context: AsyncRPCContext,
        commandID: Long
    ): ControlInvocation = {
      new ControlInvocation(method.getBareMethodName, payload, context, commandID)
    }

    def apply(
        methodName: String,
        payload: ControlRequest,
        context: AsyncRPCContext,
        commandID: Long
    ): ControlInvocation = {
      new ControlInvocation(methodName, payload, context, commandID)
    }
  }

  /**
    * Creates a dynamic proxy for the specified type `T`, which intercepts method calls
    * and sends them as ControlInvocation messages via the provided output gateway.
    */
  def createProxy[T](
      createPromise: () => (Promise[ControlReturn], Long),
      outputGateway: NetworkOutputGateway
  )(implicit ct: ClassTag[T]): T = {
    val handler = new InvocationHandler {

      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
        val (p, pid) = createPromise()
        val context = args(1).asInstanceOf[AsyncRPCContext]
        val msg = args(0).asInstanceOf[ControlRequest]
        outputGateway.sendTo(context.receiver, ControlInvocation(method.getName, msg, context, pid))
        p
      }
    }

    Proxy
      .newProxyInstance(
        getClassLoader(ct.runtimeClass),
        Array(ct.runtimeClass),
        handler
      )
      .asInstanceOf[T]
  }

  // Helper to get the correct class loader
  private def getClassLoader(cls: Class[_]): ClassLoader = {
    Option(cls.getClassLoader).getOrElse(ClassLoader.getSystemClassLoader)
  }

}

class AsyncRPCClient(
    outputGateway: NetworkOutputGateway,
    val actorId: ActorVirtualIdentity
) extends AmberLogging {

  private val unfulfilledPromises = mutable.HashMap[Long, Promise[ControlReturn]]()
  private var promiseID = 0L
  @transient lazy val controllerInterface: ControllerServiceFs2Grpc[Future, AsyncRPCContext] =
    createProxy[ControllerServiceFs2Grpc[Future, AsyncRPCContext]](createPromise, outputGateway)
  @transient lazy val workerInterface: WorkerServiceFs2Grpc[Future, AsyncRPCContext] =
    createProxy[WorkerServiceFs2Grpc[Future, AsyncRPCContext]](createPromise, outputGateway)

  def mkContext(to: ActorVirtualIdentity): AsyncRPCContext = AsyncRPCContext(actorId, to)

  protected def createPromise(): (Promise[ControlReturn], Long) = {
    promiseID += 1
    val promise = new Promise[ControlReturn]()
    unfulfilledPromises(promiseID) = promise
    (promise, promiseID)
  }

  def createInvocation(
      methodName: String,
      message: ControlRequest,
      context: AsyncRPCContext
  ): (ControlInvocation, Future[ControlReturn]) = {
    val (p, pid) = createPromise()
    (ControlInvocation(methodName, message, context, pid), p)
  }

  def sendChannelMarker(
      markerId: ChannelMarkerIdentity,
      markerType: ChannelMarkerType,
      scope: Set[ChannelIdentity],
      cmdMapping: Map[String, ControlInvocation],
      channelId: ChannelIdentity
  ): Unit = {
    logger.debug(s"send marker: $markerId to $channelId")
    outputGateway.sendTo(
      channelId,
      ChannelMarkerPayload(markerId, markerType, scope.toSeq, cmdMapping)
    )
  }

  def sendToClient(clientEvent: ClientEvent): Unit = {
    outputGateway.sendTo(
      ChannelIdentity(actorId, CLIENT, isControl = true),
      clientEvent
    )
  }

  def fulfillPromise(ret: ReturnInvocation): Unit = {
    if (unfulfilledPromises.contains(ret.commandId)) {
      val p = unfulfilledPromises(ret.commandId)
      ret.returnValue match {
        case err: ControlError =>
          p.raise(reconstructThrowable(err))
        case other =>
          p.setValue(other)
      }
      unfulfilledPromises.remove(ret.commandId)
    }
  }

  def logControlReply(ret: ReturnInvocation, channelId: ChannelIdentity): Unit = {
    if (ret.commandId == AsyncRPCClient.IgnoreReplyAndDoNotLog) {
      return
    }
    if (ret.returnValue != null) {
      if (ret.returnValue.isInstanceOf[WorkerMetricsResponse]) {
        return
      }
      logger.debug(
        s"receive reply: ${ret.returnValue.getClass.getSimpleName} from $channelId (controlID: ${ret.commandId})"
      )
      ret.returnValue match {
        case err: ControlError =>
          logger.error(s"received error from $channelId", err)
        case _ =>
      }
    } else {
      logger.info(
        s"receive reply: null from $channelId (controlID: ${ret.commandId})"
      )
    }
  }

}
