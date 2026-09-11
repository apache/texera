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

package org.apache.texera.amber.storage

import org.apache.texera.amber.core.storage.RepositoryMountManager
import org.apache.texera.common.config.EnvironmentalVariable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Path, Paths}
import scala.collection.mutable

class RepositoryMountManagerSpec extends AnyFlatSpec with Matchers {

  private val locator = "dataset-1:abc123"
  private val inPodPath = Paths.get("/mnt/texera-mounts/dataset-1/abc123")

  private val podEnv = Map(
    EnvironmentalVariable.ENV_MOUNT_IN_POD_ROOT -> "/mnt/texera-mounts",
    EnvironmentalVariable.ENV_ACCESS_CONTROL_SERVICE_URL -> "http://access-control-service-svc:9096",
    EnvironmentalVariable.ENV_CU_ID -> "7",
    EnvironmentalVariable.ENV_USER_JWT_TOKEN -> "user-jwt"
  )

  private class Fixture(
      env: Map[String, String] = podEnv,
      mountedAfterRequest: Boolean = true,
      alreadyMounted: Boolean = false,
      timeoutMs: Long = 400
  ) {
    val requests: mutable.Buffer[(String, String, String)] = mutable.Buffer()
    private var mounted = alreadyMounted

    val manager: RepositoryMountManager = new RepositoryMountManager(
      env.get,
      (url, body, jwt) => {
        requests += ((url, body, jwt))
        if (mountedAfterRequest) mounted = true
      },
      (_: Path) => mounted,
      timeoutMs
    )
  }

  "mountPointOf" should "place a locator under the pod's mount root" in {
    new Fixture().manager.mountPointOf(locator) shouldBe inPodPath
  }

  it should "fall back to the default root when the pod was given none" in {
    val fixture = new Fixture(env = podEnv - EnvironmentalVariable.ENV_MOUNT_IN_POD_ROOT)
    fixture.manager.mountPointOf(locator) shouldBe inPodPath
  }

  it should "reject a locator that is not <repositoryName>:<commitHash>" in {
    val manager = new Fixture().manager
    Seq("dataset-1", "", ":abc", "dataset-1:", null).foreach { bad =>
      an[IllegalArgumentException] should be thrownBy manager.mountPointOf(bad)
    }
  }

  "ensureMounted" should "ask the authority and return the in-pod path" in {
    val fixture = new Fixture()
    fixture.manager.ensureMounted(locator) shouldBe inPodPath

    val (url, body, jwt) = fixture.requests.head
    // The authority is addressed per computing unit; the pod's own cuid is a claim it checks.
    url shouldBe "http://access-control-service-svc:9096/api/mounts/7"
    body should include(""""repositoryName":"dataset-1"""")
    body should include(""""commitHash":"abc123"""")
    // The pod's user token: the authority re-checks that user, and GeeseFS presents it on
    // every read afterwards.
    jwt shouldBe "user-jwt"
  }

  it should "not ask again for something already mounted" in {
    val fixture = new Fixture(alreadyMounted = true)
    fixture.manager.ensureMounted(locator) shouldBe inPodPath
    fixture.requests shouldBe empty
  }

  it should "ask once per distinct locator" in {
    val fixture = new Fixture()
    fixture.manager.ensureAllMounted(Set(locator, locator))
    fixture.requests should have size 1
  }

  it should "give up if the mount never propagates into the pod" in {
    val fixture = new Fixture(mountedAfterRequest = false)
    val failure = the[RuntimeException] thrownBy fixture.manager.ensureMounted(locator)
    failure.getMessage should include("did not appear as a mount")
  }

  it should "say which variable is missing rather than fail obscurely" in {
    Seq(
      EnvironmentalVariable.ENV_ACCESS_CONTROL_SERVICE_URL,
      EnvironmentalVariable.ENV_CU_ID,
      EnvironmentalVariable.ENV_USER_JWT_TOKEN
    ).foreach { missing =>
      val fixture = new Fixture(env = podEnv - missing)
      val failure = the[IllegalStateException] thrownBy fixture.manager.ensureMounted(locator)
      failure.getMessage should include(missing)
      fixture.requests shouldBe empty
    }
  }
}
