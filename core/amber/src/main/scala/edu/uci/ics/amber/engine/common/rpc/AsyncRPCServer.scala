package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ControlInvocation
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{ControlReturn, ReturnInvocation}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.error.ErrorUtils.mkControlError

import java.lang.reflect.Method
import scala.collection.concurrent.TrieMap

class AsyncRPCServer(
    outputGateway: NetworkOutputGateway,
    val actorId: ActorVirtualIdentity
) extends AmberLogging {

  var handler: AnyRef = null

  // Define the MethodKey case class
  private case class MethodKey(name: String, paramTypes: Seq[Class[_]])

  // Cache to store methods based on method name and parameter types
  private val methodCache: TrieMap[MethodKey, Method] = TrieMap()

  // Secondary cache mapping method names to methods
  private lazy val methodsByName: Map[String, Seq[Method]] = {
    handler.getClass.getMethods.groupBy(_.getName).view.mapValues(_.toSeq).toMap
  }

  def receive(request: ControlInvocation, senderID: ActorVirtualIdentity): Unit = {
    val methodName = request.methodName
    val requestArg = request.command
    val contextArg = request.context
    val id = request.commandId
    logger.debug(
      s"receive command: ${methodName} from $senderID (controlID: ${id})"
    )

    val paramTypes = Seq(requestArg.getClass, contextArg.getClass)
    val key = MethodKey(methodName, paramTypes)

    methodCache.get(key) match {
      case Some(method) =>
        // Exact match found
        invokeMethod(method, requestArg, contextArg, id, senderID)
      case None =>
        // Attempt to find a compatible method
        methodsByName.get(methodName) match {
          case Some(methods) =>
            // Find a method with assignable parameter types
            val compatibleMethod = methods.find { method =>
              val methodParamTypes = method.getParameterTypes
              methodParamTypes.length == 2 &&
                methodParamTypes(0).isAssignableFrom(requestArg.getClass) &&
                methodParamTypes(1).isAssignableFrom(contextArg.getClass)
            }
            compatibleMethod match {
              case Some(method) =>
                // Cache this method for future calls
                methodCache.put(key, method)
                invokeMethod(method, requestArg, contextArg, id, senderID)
              case None =>
                logger.error(s"No compatible method found for $methodName with provided arguments")
            }
          case None =>
            logger.error(s"No methods found with name $methodName")
        }
    }
  }

  private def invokeMethod(method: Method, requestArg: Any, contextArg: Any, id: Long, senderID:ActorVirtualIdentity): Unit = {
    try {
      val result = method.invoke(handler, requestArg.asInstanceOf[AnyRef], contextArg.asInstanceOf[AnyRef])
      result.asInstanceOf[Future[ControlReturn]].onSuccess { ret =>
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
