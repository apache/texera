package edu.uci.ics.amber.engine.common.client

import akka.actor.{Actor, ActorRef}
import akka.pattern.StatusReply.Ack
import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage
}
import edu.uci.ics.amber.engine.common.ambermessage.{
  WorkflowControlMessage,
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

import scala.collection.mutable

// TODO: Rename or refactor it since it has mixed duties (send/receive messages, execute callbacks)
private[client] object ClientActor {
  case class InitializeRequest(workflow: Workflow, controllerConfig: ControllerConfig)
  case class ObservableRequest(pf: PartialFunction[Any, Unit])
  case class ClosureRequest[T](closure: () => T)
  case class CommandRequest(command: ControlCommand[_], promise: Promise[Any])
}

private[client] class ClientActor extends Actor {
  var controller: ActorRef = _
  var controlId = 0L
  val promiseMap = new mutable.LongMap[Promise[Any]]()
  var handlers: PartialFunction[Any, Unit] = PartialFunction.empty

  override def receive: Receive = {
    case InitializeRequest(workflow, controllerConfig) =>
      assert(controller == null)
      controller = context.actorOf(Controller.props(workflow, controllerConfig))
      sender ! Ack
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
          _ @WorkflowControlMessage(_, _, _ @ReturnInvocation(originalCommandID, controlReturn))
        ) =>
      sender ! NetworkAck(mId)
      if (handlers.isDefinedAt(controlReturn)) {
        handlers(controlReturn)
      }
      if (promiseMap.contains(originalCommandID)) {
        controlReturn match {
          case t: Throwable =>
            promiseMap(originalCommandID).setException(t)
          case other =>
            promiseMap(originalCommandID).setValue(other)
        }
        promiseMap.remove(originalCommandID)
      }
    case NetworkMessage(mId, _ @WorkflowControlMessage(_, _, _ @ControlInvocation(_, command))) =>
      sender ! NetworkAck(mId)
      if (handlers.isDefinedAt(command)) {
        handlers(command)
      }
    case x: WorkflowRecoveryMessage =>
      sender ! Ack
      controller ! x
    case other =>
      println("client actor cannot handle " + other) //skip
  }
}
