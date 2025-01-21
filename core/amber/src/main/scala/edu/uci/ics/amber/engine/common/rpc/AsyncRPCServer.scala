package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  ControlRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{ControlReturn, ReturnInvocation}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.error.ErrorUtils.mkControlError
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

import java.lang.reflect.Method
import scala.collection.mutable

class AsyncRPCServer(
    outputGateway: NetworkOutputGateway,
    val actorId: ActorVirtualIdentity
) extends AmberLogging {

  // variable to hold all handler implementations
  var handler: AnyRef = _

  // retrieve a mapping from method name to implementation
  // used transient lazy val to avoid serialization.
  @transient
  private lazy val methodsByName: Map[String, Method] = {
    val mapping = mutable.HashMap[String, Method]()
    handler.getClass.getMethods.foreach { method =>
      mapping(method.getName.toLowerCase) = method
    }
    mapping.toMap
  }

  def receive(request: ControlInvocation, senderID: ActorVirtualIdentity): Unit = {
    val methodName = request.methodName.toLowerCase
    val requestArg = request.command
    val contextArg = request.context
    val id = request.commandId
    logger.debug(
      s"receive command: ${methodName} with payload ${requestArg} from $senderID (controlID: ${id})"
    )
    methodsByName.get(methodName) match {
      case Some(method) =>
        invokeMethod(method, requestArg, contextArg, id, senderID)
      case None =>
        logger.error(s"No methods found with name $methodName")
    }
  }

  private def invokeMethod(
      method: Method,
      requestArg: ControlRequest,
      contextArg: AsyncRPCContext,
      id: Long,
      senderID: ActorVirtualIdentity
  ): Unit = {
    try {
      val result =
        try {
          method.invoke(handler, requestArg, contextArg)
        } catch {
          case e: java.lang.reflect.InvocationTargetException =>
            throw Option(e.getCause).getOrElse(e)
          case e: Throwable => throw e
        }
      result
        .asInstanceOf[Future[ControlReturn]]
        .onSuccess { ret =>
          returnResult(senderID, id, ret)
        }
        .onFailure { err =>
          logger.error("Exception occurred", err)
          returnResult(senderID, id, mkControlError(err))
        }

    } catch {
      case err: Throwable =>
        // if error occurs, return it to the sender.
        logger.error("Exception occurred", err)
        returnResult(senderID, id, mkControlError(err))
      // if throw this exception right now, the above message might not be able
      // to be sent out. We do not throw for now.
      //        throw err
    }
  }

  @inline
  private def noReplyNeeded(id: Long): Boolean = id < 0

  @inline
  private def returnResult(sender: ActorVirtualIdentity, id: Long, ret: ControlReturn): Unit = {
    if (noReplyNeeded(id)) {
      return
    }
    outputGateway.sendTo(sender, ReturnInvocation(id, ret))
  }

}
