package edu.uci.ics.amber.engine.common.control

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambermessage.neo.ControlPayload
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.control.ControlMessageSource.{
  ControlInvocation,
  ReturnPayload
}
import edu.uci.ics.amber.engine.common.control.ControlMessageReceiver.ControlCommand

import scala.collection.mutable

object ControlMessageSource {

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

class ControlMessageSource(controlOutputPort: ControlOutputPort) {

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
