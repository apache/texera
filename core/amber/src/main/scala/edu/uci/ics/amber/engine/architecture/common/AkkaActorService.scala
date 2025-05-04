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

package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{ActorContext, ActorRef, Address, Cancellable, Props}
import akka.util.Timeout
import edu.uci.ics.amber.engine.common.FutureBijection._
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class AkkaActorService(val id: ActorVirtualIdentity, actorContext: ActorContext) {

  implicit def ec: ExecutionContext = actorContext.dispatcher

  implicit val timeout: Timeout = 5.seconds
  implicit val self: ActorRef = actorContext.self

  def parent: ActorRef = actorContext.parent

  var getAvailableNodeAddressesFunc: () => Array[Address] = () => Array.empty

  def getClusterNodeAddresses: Array[Address] = {
    getAvailableNodeAddressesFunc()
  }

  def actorOf(props: Props): ActorRef = {
    actorContext.actorOf(props)
  }

  def scheduleOnce(delay: FiniteDuration, callable: () => Unit): Cancellable = {
    actorContext.system.scheduler.scheduleOnce(delay) {
      callable()
    }
  }

  def scheduleWithFixedDelay(
      initialDelay: FiniteDuration,
      delay: FiniteDuration,
      callable: () => Unit
  ): Cancellable = {
    actorContext.system.scheduler.scheduleWithFixedDelay(initialDelay, delay)(() => callable())
  }

  def sendToSelfOnce(delay: FiniteDuration, msg: Any): Cancellable = {
    actorContext.system.scheduler.scheduleOnce(delay, actorContext.self, msg)
  }

  def sendToSelfWithFixedDelay(
      initialDelay: FiniteDuration,
      delay: FiniteDuration,
      msg: Any
  ): Cancellable = {
    actorContext.system.scheduler.scheduleWithFixedDelay(
      initialDelay,
      delay,
      actorContext.self,
      msg
    )
  }

  def ask(ref: ActorRef, message: Any): com.twitter.util.Future[Any] = {
    akka.pattern.ask(ref, message).asTwitter()
  }

}
