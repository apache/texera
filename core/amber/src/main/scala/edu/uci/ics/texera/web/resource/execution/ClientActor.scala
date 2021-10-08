package edu.uci.ics.texera.web.resource.execution

import akka.actor.Actor
import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{NetworkAck, NetworkMessage}
import edu.uci.ics.amber.engine.common.ambermessage.{WorkflowControlMessage, WorkflowDataMessage, WorkflowMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ReturnInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.web.resource.execution.ClientActor.{ControlRequest, SubjectRequest}
import rx.lang.scala.Subject

object ClientActor{
  case class SubjectRequest[T](subject:Subject[T]){
    type eventType = T
  }
  case class ControlRequest[T](controlCommand: ControlCommand[T], promise:Promise[T])
}


class ClientActor extends Actor{

  private val subjects =

  override def receive: Receive = {
    case SubjectRequest(subject) =>
    case ControlRequest(control, promise) =>
    case NetworkMessage(id, workflowMessage) =>
      sender ! NetworkAck(id)

    case error:Throwable =>
    case other =>

  }


  def handleWorkflowMessage(message:WorkflowMessage): Unit ={
    message match {
      case WorkflowControlMessage(from, sequenceNumber, payload @ ReturnInvocation(originalCommandID, controlReturn)) =>

      case other =>
    }
  }

}
