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

package org.apache.texera.web.resource.pythonvirtualenvironment

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.LinkedBlockingQueue
import scala.jdk.CollectionConverters._

class PveManagerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val testCuid = 10
  private var testPveName: String = _
  private var testRoot: Path = _
  private var queue: LinkedBlockingQueue[String] = _

  override protected def beforeEach(): Unit = {
    testPveName = s"test-env-${System.currentTimeMillis()}"
    testRoot = Paths.get("/tmp/texera-pve/venvs").resolve(testCuid.toString)
    queue = new LinkedBlockingQueue[String]()
  }

  override protected def afterEach(): Unit = {
    deleteRecursively(testRoot)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (path != null && Files.exists(path)) {
      Files
        .walk(path)
        .iterator()
        .asScala
        .toList
        .sortBy(_.toString.length)
        .reverse
        .foreach(Files.deleteIfExists)
    }
  }

  private def queueMessages(): List[String] = {
    queue.iterator().asScala.toList
  }

  private def queueText(): String = {
    queueMessages().mkString("\n")
  }

  "PveManager" should "create a real pve, install a package, and uninstall it" in {
    PveManager.createNewPve(testCuid, queue, testPveName)

    val createLogs = queueText()

    createLogs should not include "[PVE][ERR]"
    PveManager.pveExists(testCuid, testPveName) shouldBe true

    val pythonPath = Paths.get(PveManager.pythonBin(testCuid, testPveName))
    val pipPath = testRoot.resolve(testPveName).resolve("pve").resolve("bin").resolve("pip")
    val metadataDir = testRoot.resolve(testPveName).resolve("pve").resolve("metadata")

    Files.exists(pythonPath) shouldBe true
    Files.exists(pipPath) shouldBe true
    Files.exists(metadataDir.resolve("system-packages.txt")) shouldBe true
    Files.exists(metadataDir.resolve("user-packages.txt")) shouldBe true

    PveManager.getEnvironments(testCuid) should contain(testPveName)

    val (_, userPackagesBeforeInstall) =
      PveManager.getSystemAndUserPackages(testCuid, testPveName)

    userPackagesBeforeInstall shouldBe empty

    val packageToInstall = "charset-normalizer==3.4.1"

    PveManager.installPackages(
      List(packageToInstall),
      testCuid,
      queue,
      testPveName
    )

    val (_, userPackagesAfterInstall) =
      PveManager.getSystemAndUserPackages(testCuid, testPveName)

    userPackagesAfterInstall.exists(_.startsWith("charset-normalizer==")) shouldBe true

    val uninstallResult =
      PveManager.deletePackages(testCuid, "charset-normalizer", testPveName)

    uninstallResult.exists(
      _.toLowerCase.contains("uninstalled charset-normalizer successfully")
    ) shouldBe true

    val (_, userPackagesAfterDelete) =
      PveManager.getSystemAndUserPackages(testCuid, testPveName)

    userPackagesAfterDelete.exists(_.startsWith("charset-normalizer==")) shouldBe false
  }
}
