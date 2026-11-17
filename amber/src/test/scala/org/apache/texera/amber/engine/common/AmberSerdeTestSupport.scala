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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Mixin for specs that drive [[AmberRuntime.serde]] with a suite-local Pekko
  * `ActorSystem`. The system is built on first use, so a suite whose tests are all
  * filtered out (which skips `beforeAll` and `afterAll`) never starts one, and
  * `afterAll` restores whatever runtime the suite found.
  */
trait AmberSerdeTestSupport extends BeforeAndAfterAll { this: Suite =>

  protected lazy val testSystem: ActorSystem =
    ActorSystem(s"${getClass.getSimpleName}-test", AmberRuntime.pekkoConfig)
  protected lazy val testSerde: Serialization = SerializationExtension(testSystem)

  private var previousActorSystem: AnyRef = _
  private var previousSerde: AnyRef = _

  protected def getAmberRuntimeField(name: String): AnyRef = {
    val field = AmberRuntime.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(AmberRuntime)
  }

  protected def setAmberRuntimeField(name: String, value: AnyRef): Unit = {
    val field = AmberRuntime.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.set(AmberRuntime, value)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    previousActorSystem = getAmberRuntimeField("_actorSystem")
    previousSerde = getAmberRuntimeField("_serde")
    setAmberRuntimeField("_actorSystem", testSystem)
    setAmberRuntimeField("_serde", testSerde)
  }

  override protected def afterAll(): Unit = {
    setAmberRuntimeField("_serde", previousSerde)
    setAmberRuntimeField("_actorSystem", previousActorSystem)
    TestKit.shutdownActorSystem(testSystem)
    super.afterAll()
  }
}
