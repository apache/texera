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

package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.Cancellable
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_FLUSH_NETWORK_BUFFER
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF

import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

class WorkerTimerService(actorService: AkkaActorService) {

  private val enabledAdaptiveBatching = AmberConfig.enableAdaptiveNetworkBuffering
  private val adaptiveBatchInterval = AmberConfig.adaptiveBufferingTimeoutMs

  var adaptiveBatchingHandle: Option[Cancellable] = None
  var isPaused = false

  def startAdaptiveBatching(): Unit = {
    if (!enabledAdaptiveBatching) {
      return
    }
    if (this.adaptiveBatchingHandle.nonEmpty) {
      return
    }
    this.adaptiveBatchingHandle = Some(
      actorService.sendToSelfWithFixedDelay(
        0.milliseconds,
        FiniteDuration.apply(adaptiveBatchInterval, MILLISECONDS),
        ControlInvocation(
          METHOD_FLUSH_NETWORK_BUFFER, // uses method descriptor instead of method name string
          EmptyRequest(),
          AsyncRPCContext(SELF, SELF),
          AsyncRPCClient.IgnoreReplyAndDoNotLog
        )
      )
    )
  }

  def stopAdaptiveBatching(): Unit = {
    if (adaptiveBatchingHandle.nonEmpty) {
      adaptiveBatchingHandle.get.cancel()
    }
    isPaused = false
  }

  def pauseAdaptiveBatching(): Unit = {
    stopAdaptiveBatching()
    isPaused = true
  }

  def resumeAdaptiveBatching(): Unit = {
    if (isPaused) {
      startAdaptiveBatching()
    }
  }
}
