package edu.uci.ics.amber.engine.common.client

import akka.actor.{ActorSystem, Address, PoisonPill, Props}
import akka.pattern._
import akka.util.Timeout
import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.core.storage.result.OpResultStorage
import edu.uci.ics.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ControlRequest
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceFs2Grpc
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.ControlReturn
import edu.uci.ics.amber.engine.common.FutureBijection._
import edu.uci.ics.amber.engine.common.ambermessage.{NotifyFailedNode, WorkflowRecoveryMessage}
import edu.uci.ics.amber.engine.common.client.ClientActor.{
  CommandRequest,
  InitializeRequest,
  ObservableRequest
}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CLIENT
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.PublishSubject

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag

class AmberClient(
    system: ActorSystem,
    workflowContext: WorkflowContext,
    physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage,
    controllerConfig: ControllerConfig,
    errorHandler: Throwable => Unit
) {

  private val clientActor = system.actorOf(Props(new ClientActor))
  private implicit val timeout: Timeout = Timeout(1.minute)
  private val registeredObservables = new mutable.HashMap[Class[_], Observable[_]]()
  @volatile private var isActive = true

  Await.result(
    clientActor ? InitializeRequest(
      workflowContext,
      physicalPlan,
      opResultStorage,
      controllerConfig
    ),
    10.seconds
  )

  def shutdown(): Unit = {
    if (isActive) {
      isActive = false
      clientActor ! PoisonPill
    }
  }

  val controllerInterface: ControllerServiceFs2Grpc[Future, Unit] =
    createProxy[ControllerServiceFs2Grpc[Future, Unit]]()

  private def createProxy[T]()(implicit ct: ClassTag[T]): T = {
    val handler = new InvocationHandler {

      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
        val req = args(0).asInstanceOf[ControlRequest]
        val p = Promise[ControlReturn]()
        clientActor ! CommandRequest(method.getName, req, p)
        p
      }
    }

    Proxy
      .newProxyInstance(
        getClassLoader(ct.runtimeClass),
        Array(ct.runtimeClass),
        handler
      )
      .asInstanceOf[T]
  }

  private def getClassLoader(cls: Class[_]): ClassLoader = {
    Option(cls.getClassLoader).getOrElse(ClassLoader.getSystemClassLoader)
  }

  def notifyNodeFailure(address: Address): Future[Any] = {
    if (!isActive) {
      Future[Any](())
    } else {
      (clientActor ? WorkflowRecoveryMessage(CLIENT, NotifyFailedNode(address))).asTwitter()
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
    observable.subscribe { evt: T =>
      {
        try {
          callback(evt)
        } catch {
          case t: Throwable => errorHandler(t)
        }
      }
    }
  }

}
