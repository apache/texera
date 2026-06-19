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

package org.apache.texera.amber.util.serde

import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PortIdentityKeySerializerSpec extends AnyFlatSpec with Matchers {

  "PortIdentityKeySerializer.portIdToString" should "format PortIdentity as id_internal" in {
    PortIdentityKeySerializer
      .portIdToString(PortIdentity(3, internal = false)) shouldBe "3_false"
    PortIdentityKeySerializer
      .portIdToString(PortIdentity(0, internal = true)) shouldBe "0_true"
  }

  it should "preserve zero, large, negative, internal, and external ports in the key string" in {
    val cases = Seq(
      PortIdentity(0, internal = false) -> "0_false",
      PortIdentity(0, internal = true) -> "0_true",
      PortIdentity(999999, internal = false) -> "999999_false",
      PortIdentity(999999, internal = true) -> "999999_true",
      PortIdentity(-1, internal = false) -> "-1_false",
      PortIdentity(-1, internal = true) -> "-1_true"
    )

    cases.foreach {
      case (portIdentity, expected) =>
        PortIdentityKeySerializer.portIdToString(portIdentity) shouldBe expected
    }
  }

  "PortIdentityKeySerializer" should "write Jackson map keys with the id_internal format" in {
    val original = Map(
      PortIdentity(3, internal = false) -> "external",
      PortIdentity(0, internal = true) -> "internal"
    )

    val json = objectMapper.writeValueAsString(original)

    json should include(""""3_false"""")
    json should include(""""0_true"""")
  }
}
