/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.web.service

import com.twitter.util.Future
import io.reactivex.rxjava3.disposables.Disposable
import org.apache.pekko.actor.ActorSystem
import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.CoordinatorConfig
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceFs2Grpc
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.web.model.websocket.event.TexeraWebSocketEvent
import org.apache.texera.web.storage.StateStore

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.collection.mutable
import scala.reflect.ClassTag

object RecordingExecutionClient {

  /** Collects every websocket event batch emitted by `store` while `body` runs. */
  def recordBatches[S](
      store: StateStore[S]
  )(body: => Unit): Seq[Iterable[TexeraWebSocketEvent]] = {
    val batches = mutable.ListBuffer.empty[Iterable[TexeraWebSocketEvent]]
    val subscription = store.getWebsocketEventObservable
      .subscribe((batch: Iterable[TexeraWebSocketEvent]) => batches += batch)
    try body
    finally subscription.dispose()
    batches.toSeq
  }
}

/**
  * Empty-plan `AmberClient` for execution-service unit specs: captures registered callbacks,
  * honors subscription disposal, and optionally records coordinator RPC invocations.
  */
class RecordingExecutionClient(
    system: ActorSystem,
    customCoordinator: Option[CoordinatorServiceFs2Grpc[Future, Unit]] = None
) extends AmberClient(
      system,
      new WorkflowContext(),
      PhysicalPlan(Set.empty, Set.empty),
      CoordinatorConfig(None, None, None, None),
      _ => ()
    ) {

  var shutdownCount = 0
  private val callbacks = mutable.Map.empty[Class[_], mutable.ArrayBuffer[Any => Unit]]

  /** `(method name, request)` of every call made through `coordinatorInterface` when no mock is provided. */
  val coordinatorCalls: mutable.ArrayBuffer[(String, Any)] = mutable.ArrayBuffer.empty

  /** Event class of every `Disposable` handed out by `registerCallback` that was later disposed. */
  val disposedCallbacks: mutable.ArrayBuffer[Class[_]] = mutable.ArrayBuffer.empty

  override val coordinatorInterface: CoordinatorServiceFs2Grpc[Future, Unit] =
    customCoordinator.getOrElse(
      Proxy
        .newProxyInstance(
          classOf[CoordinatorServiceFs2Grpc[Future, Unit]].getClassLoader,
          Array(classOf[CoordinatorServiceFs2Grpc[Future, Unit]]),
          new InvocationHandler {
            override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
              if (args == null || args.isEmpty) {
                if (method.getName == "hashCode") Integer.valueOf(System.identityHashCode(proxy))
                else if (method.getName == "toString") "TestCoordinatorInterface"
                else null
              } else {
                coordinatorCalls += ((method.getName, args(0)))
                Future.value(EmptyReturn())
              }
            }
          }
        )
        .asInstanceOf[CoordinatorServiceFs2Grpc[Future, Unit]]
    )

  override def shutdown(): Unit = {
    shutdownCount += 1
    super.shutdown()
  }

  override def registerCallback[T](callback: T => Unit)(implicit ct: ClassTag[T]): Disposable = {
    val clazz = ct.runtimeClass
    val registered = callback.asInstanceOf[Any => Unit]
    callbacks.getOrElseUpdate(clazz, mutable.ArrayBuffer.empty) += registered
    Disposable.fromAction(() => {
      callbacks.get(clazz).foreach { registrations =>
        registrations -= registered
        if (registrations.isEmpty) callbacks.remove(clazz)
      }
      disposedCallbacks += clazz
    })
  }

  def hasCallback[T](implicit ct: ClassTag[T]): Boolean =
    callbacks.contains(ct.runtimeClass)

  /**
    * Delivers an engine event to every callback registered for its class, in registration
    * order, the way the client's observable would.
    */
  def fire[T <: AnyRef](event: T)(implicit ct: ClassTag[T]): Unit = {
    if (!hasCallback[T]) {
      throw new AssertionError(s"no callback is registered for ${ct.runtimeClass.getSimpleName}")
    }
    fireIfRegistered(event)
  }

  /** Same delivery as `fire`, tolerating absent callbacks (e.g. after teardown). */
  def fireIfRegistered[T <: AnyRef](event: T)(implicit ct: ClassTag[T]): Unit =
    callbacks.get(ct.runtimeClass).foreach(_.toList.foreach(deliver => deliver(event)))

  def reset(): Unit = {
    callbacks.clear()
    coordinatorCalls.clear()
    disposedCallbacks.clear()
  }

  def dispose(): Unit = super.shutdown()
}
