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
import javax.ws.rs.BadRequestException
import scala.jdk.CollectionConverters._

class PveResourceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val resource = new PveResource()
  private val testCuid = 10
  private val testRoot: Path = Paths.get("/tmp/texera-pve/venvs").resolve(testCuid.toString)

  override protected def afterEach(): Unit = {
    deleteRecursively(testRoot)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
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

  "createPve" should "throw BadRequestException when cuid is invalid" in {
    intercept[BadRequestException] {
      resource.createPve("[]", 0, "test-env")
    }
  }

  it should "throw BadRequestException when environment name is missing" in {
    intercept[BadRequestException] {
      resource.createPve("[]", testCuid, "")
    }
  }

  "getInstalledPackages" should "throw BadRequestException when cuid is invalid" in {
    intercept[BadRequestException] {
      resource.getInstalledPackages(0, "test-env")
    }
  }

  it should "throw BadRequestException when environment name is missing" in {
    intercept[BadRequestException] {
      resource.getInstalledPackages(testCuid, "")
    }
  }

  it should "return empty system and user package lists when metadata does not exist" in {
    val result = resource.getInstalledPackages(testCuid, "test-env")

    result.get("system").asScala.toList shouldBe List.empty
    result.get("user").asScala.toList shouldBe List.empty
  }

  "getEnvironments" should "return an empty list when the user has no environments" in {
    resource.getEnvironments(testCuid).asScala.toList shouldBe List.empty
  }

  it should "return the list of environment names for the given cuid" in {
    Files.createDirectories(testRoot.resolve("env1"))
    Files.createDirectories(testRoot.resolve("env2"))

    val result = resource.getEnvironments(testCuid).asScala.toSet

    result shouldBe Set("env1", "env2")
  }

  "uninstallPackage" should "throw BadRequestException when environment name is missing" in {
    intercept[BadRequestException] {
      resource.uninstallPackage("numpy", testCuid, "")
    }
  }

  it should "return an error message when pip does not exist" in {
    val result = resource.uninstallPackage("numpy", testCuid, "test-env").asScala.toList

    result.head should include("No pip found")
  }
}
