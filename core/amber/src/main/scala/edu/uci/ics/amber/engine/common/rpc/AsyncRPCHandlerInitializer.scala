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

package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ClientEvent
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceFs2Grpc
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns._
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceFs2Grpc
import edu.uci.ics.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}

import scala.language.implicitConversions

class AsyncRPCHandlerInitializer(
    ctrlSource: AsyncRPCClient,
    ctrlReceiver: AsyncRPCServer
) {
  implicit def returnAsFuture[R](ret: R): Future[R] = Future[R](ret)

  implicit def actorIdAsContext(to: ActorVirtualIdentity): AsyncRPCContext = mkContext(to)

  implicit def stringToResponse(s: String): StringResponse = StringResponse(s)

  implicit def intToResponse(i: Int): IntResponse = IntResponse(i)

  // register all handlers
  ctrlReceiver.handler = this

  def controllerInterface: ControllerServiceFs2Grpc[Future, AsyncRPCContext] =
    ctrlSource.controllerInterface

  def workerInterface: WorkerServiceFs2Grpc[Future, AsyncRPCContext] = ctrlSource.workerInterface

  def mkContext(to: ActorVirtualIdentity): AsyncRPCContext = ctrlSource.mkContext(to)

  def sendECM(
      ecmId: EmbeddedControlMessageIdentity,
      ecmType: EmbeddedControlMessageType,
      scope: Set[ChannelIdentity],
      cmdMapping: Map[String, ControlInvocation],
      to: ChannelIdentity
  ): Unit = {
    ctrlSource.sendECMToChannel(ecmId, ecmType, scope, cmdMapping, to)
  }

  def sendToClient(clientEvent: ClientEvent): Unit = {
    ctrlSource.sendToClient(clientEvent)
  }

  def createInvocation(
      methodName: String,
      payload: ControlRequest,
      to: ActorVirtualIdentity
  ): (ControlInvocation, Future[ControlReturn]) =
    ctrlSource.createInvocation(methodName, payload, ctrlSource.mkContext(to))

}
