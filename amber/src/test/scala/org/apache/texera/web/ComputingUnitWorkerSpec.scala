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

package org.apache.texera.web

import org.apache.commons.jcs3.access.exception.InvalidArgumentException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ComputingUnitWorkerSpec extends AnyFlatSpec with Matchers {

  "parseArgs" should "return no options when the worker receives no arguments" in {
    ComputingUnitWorker.parseArgs(Array.empty[String]) shouldBe Map.empty
  }

  it should "parse the worker server address without changing its value" in {
    ComputingUnitWorker.parseArgs(Array("--serverAddr", "worker.internal:8090")) shouldBe
      Map(Symbol("serverAddr") -> "worker.internal:8090")
  }

  it should "use the last server address when the option is repeated" in {
    ComputingUnitWorker.parseArgs(
      Array("--serverAddr", "first:8080", "--serverAddr", "last:9090")
    ) shouldBe
      Map(Symbol("serverAddr") -> "last:9090")
  }

  it should "reject an unknown command-line option" in {
    val exception = intercept[InvalidArgumentException] {
      ComputingUnitWorker.parseArgs(Array("--cluster", "true"))
    }

    exception.getMessage shouldBe "unknown command-line arg"
  }

  it should "reject a server address option with no value" in {
    val exception = intercept[InvalidArgumentException] {
      ComputingUnitWorker.parseArgs(Array("--serverAddr"))
    }

    exception.getMessage shouldBe "unknown command-line arg"
  }
}
