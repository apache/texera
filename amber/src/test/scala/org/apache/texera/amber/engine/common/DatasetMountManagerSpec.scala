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

class DatasetMountManagerSpec extends AnyFlatSpec with Matchers {

  "ensureMounted" should "reject a locator that is not <repositoryName>:<commitHash>" in {
    for (bad <- Seq("no-colon", "", ":", "repo:", ":commit")) {
      withClue(s"locator '$bad': ") {
        an[IllegalArgumentException] should be thrownBy DatasetMountManager.ensureMounted(bad)
      }
    }
  }

  it should "not attempt a mount for a well-formed, unmounted locator without the pod's mount identity" in {
    // The locator is valid and no such mount exists in this test process, so ensureMounted
    // advances past parsing and the /proc/mounts check to the environment checks (user JWT /
    // node IP), which must fail loudly rather than attempt an unauthorized/unroutable mount.
    val ex = intercept[RuntimeException] {
      DatasetMountManager.ensureMounted("dataset-does-not-exist:deadbeefdeadbeef")
    }
    ex.getMessage.toLowerCase should include("computing unit")
  }
}
