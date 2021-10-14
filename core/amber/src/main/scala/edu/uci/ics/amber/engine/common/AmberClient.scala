package edu.uci.ics.amber.engine.common

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.pattern._
import akka.util.Timeout
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  ErrorOccurred,
  WorkflowCompleted
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage
}
import edu.uci.ics.amber.engine.common.FutureBijection._
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowControlMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import rx.lang.scala.{Observable, Subject}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}
import scala.reflect.ClassTag

class AmberClient(system: ActorSystem, workflow: Workflow, controllerConfig: ControllerConfig) {

  private case class CommandRequest[T](controlCommand: ControlCommand[T])
  private case class ObservableRequest(pf: PartialFunction[Any, Unit])
  private val client = system.actorOf(Props(new ClientActor))
  private implicit val timeout: Timeout = Timeout(1.minute)
  private val registeredSubjects = new mutable.HashMap[Class[_], Subject[_]]()
  @volatile private var isActive = true

  getObservable[WorkflowCompleted].subscribe(evt => {
    shutdown()
  })

  getObservable[ErrorOccurred].subscribe(evt => {
    shutdown()
  })

  getObservable[FatalError].subscribe(evt => {
    shutdown()
  })

  class ClientActor extends Actor {
    val controller: ActorRef = context.actorOf(Controller.props(workflow, controllerConfig))
    var controlId = 0L
    val senderMap = new mutable.LongMap[ActorRef]()
    var handlers: PartialFunction[Any, Unit] = PartialFunction.empty

    override def receive: Receive = {
      case CommandRequest(controlCommand) =>
        controller ! ControlInvocation(controlId, controlCommand)
        senderMap(controlId) = sender
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
        if (senderMap.contains(originalCommandID)) {
          senderMap(originalCommandID) ! controlReturn
          senderMap.remove(originalCommandID)
        }
      case NetworkMessage(mId, _ @WorkflowControlMessage(_, _, _ @ControlInvocation(_, command))) =>
        sender ! NetworkAck(mId)
        if (handlers.isDefinedAt(command)) {
          handlers(command)
        }
      case other =>
        println(other) //skip
    }
  }

  def shutdown(): Unit = {
    if (isActive) {
      isActive = false
      client ! PoisonPill
    }
  }

  def sendAsync[T](controlCommand: ControlCommand[T]): Future[T] = {
    if (!isActive) {
      Future.exception(new RuntimeException("amber runtime environment is not active"))
    } else {
      (client ? CommandRequest(controlCommand)).asTwitter().asInstanceOf[Future[T]]
    }
  }

  def sendSync[T](controlCommand: ControlCommand[T], deadline: Duration = timeout.duration): T = {
    if (!isActive) {
      throw new RuntimeException("amber runtime environment is not active")
    } else {
      Await.result(client ? CommandRequest(controlCommand), deadline).asInstanceOf[T]
    }
  }

  def fireAndForget[T](controlCommand: ControlCommand[T]): Unit = {
    if (!isActive) {
      throw new RuntimeException("amber runtime environment is not active")
    } else {
      client ! CommandRequest(controlCommand)
    }
  }

  def getObservable[T](implicit ct: ClassTag[T]): Observable[T] = {
    if (!isActive) {
      throw new RuntimeException("amber runtime environment is not active")
    }
    assert(
      client.path.address.hasLocalScope,
      "get observable with a remote client actor is not supported"
    )
    val clazz = ct.runtimeClass
    if (registeredSubjects.contains(clazz)) {
      return registeredSubjects(clazz).asInstanceOf[Observable[T]]
    }
    val ob = Subject[T]
    val req = ObservableRequest({
      case x: T =>
        ob.onNext(x)
    })
    Await.result(client ? req, 2.seconds)
    registeredSubjects(clazz) = ob
    ob
  }
}
