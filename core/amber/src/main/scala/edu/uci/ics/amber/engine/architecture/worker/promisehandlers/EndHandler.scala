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

package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import akka.actor.ActorRef
import akka.pattern.gracefulStop
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EndWorkerRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EndWorkerResponse
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.FutureBijection._

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}

trait EndHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def endWorker(
      request: EndWorkerRequest,
      ctx: AsyncRPCContext
  ): Future[EndWorkerResponse] = {
    val selection = AmberRuntime.actorSystem.actorSelection(request.actorRefStr)
    val actorRef: ActorRef = Await.result(selection.resolveOne(3.seconds), 3.seconds)
    gracefulStop(actorRef, Duration(5, TimeUnit.SECONDS))
      .asTwitter()
      .map(f => EndWorkerResponse(f))
  }
}
