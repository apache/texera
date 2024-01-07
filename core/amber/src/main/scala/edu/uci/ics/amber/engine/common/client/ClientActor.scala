package edu.uci.ics.amber.engine.common.client

import akka.actor.{Actor, ActorRef}
import akka.pattern.StatusReply.Ack
import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.{
  CreditRequest,
  CreditResponse,
  NetworkAck,
  NetworkMessage
}
import edu.uci.ics.amber.engine.architecture.controller.Controller.WorkflowRecoveryStatus
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.{
  ChannelID,
  ControlPayload,
  DataPayload,
  WorkflowFIFOMessage,
  WorkflowRecoveryMessage
}
import edu.uci.ics.amber.engine.common.client.ClientActor.{
  ClosureRequest,
  CommandRequest,
  InitializeRequest,
  ObservableRequest
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

// TODO: Rename or refactor it since it has mixed duties (send/receive messages, execute callbacks)
private[client] object ClientActor {
  case class InitializeRequest(workflow: Workflow, controllerConfig: ControllerConfig)
  case class ObservableRequest(pf: PartialFunction[Any, Unit])
  case class ClosureRequest[T](closure: () => T)
  case class CommandRequest(command: ControlCommand[_], promise: Promise[Any])
}

private[client] class ClientActor extends Actor with AmberLogging {
  var actorId: ActorVirtualIdentity = ActorVirtualIdentity("Client")
  var controller: ActorRef = _
  var controlId = 0L
  val promiseMap = new mutable.LongMap[Promise[Any]]()
  var handlers: PartialFunction[Any, Unit] = PartialFunction.empty

  private def getQueuedCredit(channel: ChannelID): Long = {
    0L // client does not have queued credits
  }

  private def handleControl(control: Any): Unit = {
    if (handlers.isDefinedAt(control)) {
      handlers(control)
    }
  }

  override def receive: Receive = {
    case InitializeRequest(workflow, controllerConfig) =>
      assert(controller == null)
      controller = context.actorOf(Controller.props(workflow, controllerConfig))
      sender ! Ack
    case CreditRequest(channel: ChannelID) =>
      sender ! CreditResponse(channel, getQueuedCredit(channel))
    case ClosureRequest(closure) =>
      try {
        sender ! closure()
      } catch {
        case e: Throwable =>
          sender ! e
      }
    case commandRequest: CommandRequest =>
      controller ! ControlInvocation(controlId, commandRequest.command)
      promiseMap(controlId) = commandRequest.promise
      controlId += 1
    case req: ObservableRequest =>
      handlers = req.pf orElse handlers
      sender ! scala.runtime.BoxedUnit.UNIT
    case NetworkMessage(
          mId,
          fifoMsg @ WorkflowFIFOMessage(_, _, payload)
        ) =>
      sender ! NetworkAck(mId, getInMemSize(fifoMsg), getQueuedCredit(fifoMsg.channel))
      payload match {
        case payload: ControlPayload =>
          payload match {
            case ControlInvocation(_, command) => handleControl(command)
            case ReturnInvocation(originalCommandID, controlReturn) =>
              handleControl(controlReturn)
              if (promiseMap.contains(originalCommandID)) {
                controlReturn match {
                  case t: Throwable =>
                    promiseMap(originalCommandID).setException(t)
                  case other =>
                    promiseMap(originalCommandID).setValue(other)
                }
                promiseMap.remove(originalCommandID)
              }
            case _ => ???
          }
        case _: DataPayload => ???
        case _              => ???
      }
    case x: WorkflowRecoveryMessage =>
      sender ! Ack
      controller ! x
    case x: WorkflowRecoveryStatus =>
      handleControl(x)
    case other =>
      logger.warn("client actor cannot handle " + other) //skip
  }
}
