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

import java.nio.file.{FileSystemException, Files, Path}

/**
  * Mixin for specs that drive [[AmberRuntime.serde]] with a suite-local Pekko
  * `ActorSystem`, plus best-effort recursive temp-directory cleanup.
  */
trait AmberSerdeTestSupport extends BeforeAndAfterAll { this: Suite =>

  protected def amberSystemName: String = s"${getClass.getSimpleName}-test"
  protected def lazyAmberActorSystem: Boolean = false
  protected def restorePriorAmberRuntime: Boolean = false

  protected lazy val testSystem: ActorSystem =
    ActorSystem(amberSystemName, AmberRuntime.pekkoConfig)
  protected lazy val testSerde: Serialization = SerializationExtension(testSystem)

  if (!lazyAmberActorSystem) {
    val _ = testSerde
  }

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
    if (restorePriorAmberRuntime) {
      previousActorSystem = getAmberRuntimeField("_actorSystem")
      previousSerde = getAmberRuntimeField("_serde")
    }
    setAmberRuntimeField("_actorSystem", testSystem)
    setAmberRuntimeField("_serde", testSerde)
  }

  override protected def afterAll(): Unit = {
    if (restorePriorAmberRuntime) {
      setAmberRuntimeField("_serde", previousSerde)
      setAmberRuntimeField("_actorSystem", previousActorSystem)
    } else {
      setAmberRuntimeField("_serde", null)
      setAmberRuntimeField("_actorSystem", null)
    }
    TestKit.shutdownActorSystem(testSystem)
    super.afterAll()
  }

  protected def deleteRecursively(
      path: Path,
      ignoreIOException: Boolean = false,
      ignoreWindowsFsException: Boolean = false
  ): Unit = {
    if (path == null || !Files.exists(path)) return
    val isWindows = System.getProperty("os.name", "").toLowerCase.contains("win")
    val stream = Files.walk(path)
    try {
      stream
        .sorted(java.util.Comparator.reverseOrder())
        .forEach { child =>
          try Files.deleteIfExists(child)
          catch {
            case _: FileSystemException if ignoreWindowsFsException && isWindows => ()
          }
        }
    } catch {
      case _: java.io.IOException if ignoreIOException => ()
    } finally {
      stream.close()
    }
  }
}
