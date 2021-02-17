package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.architecture.worker.WorkerStatistics
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}

import scala.collection.mutable

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

  def noReplyNeeded(id: Long): Boolean = id < 0

  final val IgnoreReply = -1

  final val IgnoreReplyAndDoNotLog = -2

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

class AsyncRPCClient(controlOutputPort: ControlOutputPort, logger: WorkflowLogger) {

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
      unfulfilledPromises.remove(ret.originalCommandID)
    }
  }

  def logControlReply(ret: ReturnPayload, sender: VirtualIdentity): Unit = {
    if (ret.originalCommandID == AsyncRPCClient.IgnoreReplyAndDoNotLog) {
      return
    }
    if (ret.returnValue != null) {
      if (ret.returnValue.isInstanceOf[WorkerStatistics]) {
        return
      }
      logger.logInfo(
        s"receive reply: ${ret.returnValue.getClass.getSimpleName} from ${sender.toString} (controlID: ${ret.originalCommandID})"
      )
    } else {
      logger.logInfo(
        s"receive reply: null from ${sender.toString} (controlID: ${ret.originalCommandID})"
      )
    }
  }

}
