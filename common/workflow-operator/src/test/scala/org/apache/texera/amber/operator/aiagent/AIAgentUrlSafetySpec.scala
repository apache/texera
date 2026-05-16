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

package org.apache.texera.amber.operator.aiagent

import org.scalatest.flatspec.AnyFlatSpec

class AIAgentUrlSafetySpec extends AnyFlatSpec {

  "AIAgentUrlSafety.validatePublicHttpUrl" should "allow public http and https URLs" in {
    assert(AIAgentUrlSafety.validatePublicHttpUrl("https://93.184.216.34/report.pdf").getHost == "93.184.216.34")
    assert(AIAgentUrlSafety.validatePublicHttpUrl("http://93.184.216.34").getScheme == "http")
  }

  it should "reject non-http schemes" in {
    val error = intercept[IllegalArgumentException] {
      AIAgentUrlSafety.validatePublicHttpUrl("file:///etc/passwd")
    }

    assert(error.getMessage.contains("http(s)"))
  }

  it should "reject localhost names" in {
    val error = intercept[IllegalArgumentException] {
      AIAgentUrlSafety.validatePublicHttpUrl("http://localhost:9000")
    }

    assert(error.getMessage.contains("Private or local"))
  }

  it should "reject private IP addresses" in {
    val error = intercept[IllegalArgumentException] {
      AIAgentUrlSafety.validatePublicHttpUrl("http://10.0.0.5/secret.pdf")
    }

    assert(error.getMessage.contains("Private or local"))
  }
}
