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

package org.apache.texera.amber.engine.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

/** The scheduling helpers return a no-op handle instead of throwing when the
  * runtime's actor system is not started.
  */
class AmberRuntimeScheduleSpec extends AnyFlatSpec with Matchers {

  // Run the body with the singleton's _actorSystem forced to null, then restore it.
  private def withNullActorSystem[T](body: => T): T = {
    val field = AmberRuntime.getClass.getDeclaredField("_actorSystem")
    field.setAccessible(true)
    val saved = field.get(AmberRuntime)
    field.set(AmberRuntime, null)
    try body
    finally field.set(AmberRuntime, saved)
  }

  "scheduleCallThroughActorSystem" should "return an already-cancelled handle instead of throwing when the runtime is not started" in {
    withNullActorSystem {
      val handle = AmberRuntime.scheduleCallThroughActorSystem(1.second)(())
      handle.isCancelled shouldBe true
    }
  }

  "scheduleRecurringCallThroughActorSystem" should "return an already-cancelled handle instead of throwing when the runtime is not started" in {
    withNullActorSystem {
      val handle = AmberRuntime.scheduleRecurringCallThroughActorSystem(1.second, 1.second)(())
      handle.isCancelled shouldBe true
    }
  }
}
