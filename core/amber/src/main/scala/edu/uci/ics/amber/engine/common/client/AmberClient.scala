package edu.uci.ics.amber.engine.common.client

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern._
import akka.util.Timeout
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.FutureBijection._
import edu.uci.ics.amber.engine.common.client.ClientActor.{ClosureRequest, InitializeRequest, ObservableRequest}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.{PublishSubject, Subject}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}
import scala.reflect.ClassTag

class AmberClient(
    system: ActorSystem,
    workflow: Workflow,
    controllerConfig: ControllerConfig,
    errorHandler: Throwable => Unit
) {

  private val clientActor = system.actorOf(Props(new ClientActor))
  private implicit val timeout: Timeout = Timeout(1.minute)
  private val registeredObservables = new mutable.HashMap[Class[_], Observable[_]]()
  @volatile private var isActive = true

  Await.result(clientActor ? InitializeRequest(workflow, controllerConfig), 10.seconds)

  def shutdown(): Unit = {
    if (isActive) {
      isActive = false
      clientActor ! PoisonPill
    }
  }

  def sendAsync[T](controlCommand: ControlCommand[T]): Future[T] = {
    if (!isActive) {
      Future.exception(new RuntimeException("amber runtime environment is not active"))
    } else {
      (clientActor ? controlCommand).asTwitter().asInstanceOf[Future[T]].onFailure(errorHandler)
    }
  }

  def sendSync[T](controlCommand: ControlCommand[T], deadline: Duration = timeout.duration): T = {
    if (!isActive) {
      throw new RuntimeException("amber runtime environment is not active")
    } else {
      val result = Await.result(clientActor ? controlCommand, deadline)
      result match {
        case t: Throwable => throw t
        case other        => other.asInstanceOf[T]
      }
    }
  }

  def fireAndForget[T](controlCommand: ControlCommand[T]): Unit = {
    if (!isActive) {
      throw new RuntimeException("amber runtime environment is not active")
    } else {
      clientActor ! controlCommand
    }
  }

  def registerCallback[T](callback: T => Unit)(implicit ct: ClassTag[T]): Disposable = {
    if (!isActive) {
      throw new RuntimeException("amber runtime environment is not active")
    }
    assert(
      clientActor.path.address.hasLocalScope,
      "get observable with a remote client actor is not supported"
    )
    val clazz = ct.runtimeClass
    val observable =
      if (registeredObservables.contains(clazz)) {
        registeredObservables(clazz).asInstanceOf[Observable[T]]
      } else {
        val sub = PublishSubject.create[T]()
        val req = ObservableRequest({
          case x: T =>
            sub.onNext(x)
        })
        Await.result(clientActor ? req, atMost = 2.seconds)
        val ob = sub.onTerminateDetach
        registeredObservables(clazz) = ob
        ob
      }
    observable.subscribe{evt:T => {
      try {
        callback(evt)
      } catch {
        case t: Throwable => errorHandler(t)
      }
    }}
  }

  def executeClosureSync[T](closure: => T): T = {
    if (!isActive) {
      closure
    } else {
      Await.result(clientActor ? ClosureRequest(() => closure), timeout.duration).asInstanceOf[T]
    }
  }

}
