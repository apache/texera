package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambermessage.neo.ControlPayload
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

import scala.collection.mutable

/** Motivation of having a separate module to handle control messages as RPCs:
  * message passing is a low level abstraction -- manually handle response
  *
  * rpc (request-response)
  * remote.callFunctionX().then(response => {
  * })
  * user-api: promise -- straightforward
  *
  * so we want to implement rpc model on top of message passing
  * goal: request-response model with multiplexing
  * client: initiate request
  * (web browser, actor that invoke control command)
  * server: handle request, return response
  * (web server, actor that handles control command)
  */
object AsyncRPCClient {

  /** The invocation of a control command
    * @param commandID
    * @param command
    */
  case class ControlInvocation(commandID: Long, command: ControlCommand[_]) extends ControlPayload

  /** The return message of a promise.
    * @param originalCommandID
    * @param returnValue
    */
  case class ReturnPayload(originalCommandID: Long, returnValue: Any) extends ControlPayload

}

class AsyncRPCClient(controlOutputPort: ControlOutputPort) {

  private var promiseID = 0L

  private val unfulfilledPromises = mutable.LongMap[WorkflowPromise[_]]()

  def send[T](cmd: ControlCommand[T], to: ActorVirtualIdentity): Future[T] = {
    val (p, id) = createPromise[T]()
    controlOutputPort.sendTo(to, ControlInvocation(id, cmd))
    p
  }

  private def createPromise[T](): (Promise[T], Long) = {
    promiseID += 1
    val promise = new WorkflowPromise[T]()
    unfulfilledPromises(promiseID) = promise
    (promise, promiseID)
  }

  def fulfillPromise(ret: ReturnPayload): Unit = {
    if (unfulfilledPromises.contains(ret.originalCommandID)) {
      val p = unfulfilledPromises(ret.originalCommandID)
      p.setValue(ret.returnValue.asInstanceOf[p.returnType])
    }
  }

}
