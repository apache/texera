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

package org.apache.texera.amber.engine.architecture.controller

import org.apache.pekko.actor.Cancellable
import org.apache.texera.amber.engine.architecture.common.AkkaActorService
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  QueryStatisticsRequest,
  StatisticsUpdateTarget
}
import org.apache.texera.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_CONTROLLER_INITIATE_QUERY_STATISTICS
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.virtualidentity.util.SELF

import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

class ControllerTimerService(
    controllerConfig: ControllerConfig,
    akkaActorService: AkkaActorService
) {
  var statusUpdateAskHandle: Option[Cancellable] = None
  var runtimeStatisticsAskHandle: Option[Cancellable] = None

  private def enableTimer(
      intervalMs: Option[Long],
      updateTarget: StatisticsUpdateTarget,
      handleOpt: Option[Cancellable]
  ): Option[Cancellable] = {
    if (intervalMs.nonEmpty && handleOpt.isEmpty) {
      Option(
        akkaActorService.sendToSelfWithFixedDelay(
          0.milliseconds,
          FiniteDuration.apply(intervalMs.get, MILLISECONDS),
          ControlInvocation(
            METHOD_CONTROLLER_INITIATE_QUERY_STATISTICS,
            QueryStatisticsRequest(Seq.empty, updateTarget),
            AsyncRPCContext(SELF, SELF),
            0
          )
        )
      )
    } else {
      handleOpt
    }
  }

  private def disableTimer(handleOpt: Option[Cancellable]): Option[Cancellable] = {
    if (handleOpt.nonEmpty) {
      handleOpt.get.cancel()
      Option.empty
    } else {
      handleOpt
    }
  }

  def enableStatusUpdate(): Unit = {
    statusUpdateAskHandle = enableTimer(
      controllerConfig.statusUpdateIntervalMs,
      StatisticsUpdateTarget.UI_ONLY,
      statusUpdateAskHandle
    )
  }

  def enableRuntimeStatisticsCollection(): Unit = {
    runtimeStatisticsAskHandle = enableTimer(
      controllerConfig.runtimeStatisticsPersistenceIntervalMs,
      StatisticsUpdateTarget.PERSISTENCE_ONLY,
      runtimeStatisticsAskHandle
    )
  }

  def disableStatusUpdate(): Unit = {
    statusUpdateAskHandle = disableTimer(statusUpdateAskHandle)
  }

  def disableRuntimeStatisticsCollection(): Unit = {
    runtimeStatisticsAskHandle = disableTimer(runtimeStatisticsAskHandle)
  }
}
