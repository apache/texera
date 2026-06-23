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

import scala.jdk.CollectionConverters._

class PortIdentityKeyDeserializerSpec extends AnyFlatSpec with Matchers {

  private val deserializer = new PortIdentityKeyDeserializer

  "PortIdentityKeyDeserializer.deserializeKey" should "parse id_internal keys into PortIdentity" in {
    deserializer.deserializeKey("3_false", null) shouldBe PortIdentity(3, internal = false)
    deserializer.deserializeKey("0_true", null) shouldBe PortIdentity(0, internal = true)
  }

  it should "round-trip serializer output for representative PortIdentity values" in {
    val cases = Seq(
      PortIdentity(0, internal = false),
      PortIdentity(0, internal = true),
      PortIdentity(999999, internal = false),
      PortIdentity(999999, internal = true),
      PortIdentity(-1, internal = false),
      PortIdentity(-1, internal = true)
    )

    cases.foreach { portIdentity =>
      deserializer.deserializeKey(
        PortIdentityKeySerializer.portIdToString(portIdentity),
        null
      ) shouldBe portIdentity
    }
  }

  it should "round-trip Map[PortIdentity, String] keys through JSONUtils.objectMapper" in {
    val original = Map(
      PortIdentity(0, internal = false) -> "zero-external",
      PortIdentity(3, internal = true) -> "three-internal",
      PortIdentity(999999, internal = false) -> "large-external",
      PortIdentity(-1, internal = true) -> "negative-internal"
    )
    val json = objectMapper.writeValueAsString(original)
    val mapType = objectMapper.getTypeFactory
      .constructMapType(classOf[java.util.HashMap[_, _]], classOf[PortIdentity], classOf[String])

    val restored: java.util.Map[PortIdentity, String] = objectMapper.readValue(json, mapType)

    restored.asScala.toMap shouldBe original
  }

  it should "round-trip an empty Map[PortIdentity, String] through JSONUtils.objectMapper" in {
    val original = Map.empty[PortIdentity, String]
    val json = objectMapper.writeValueAsString(original)
    val mapType = objectMapper.getTypeFactory
      .constructMapType(classOf[java.util.HashMap[_, _]], classOf[PortIdentity], classOf[String])

    val restored: java.util.Map[PortIdentity, String] = objectMapper.readValue(json, mapType)

    restored.asScala.toMap shouldBe original
  }

  it should "throw NumberFormatException for a non-integer id" in {
    intercept[NumberFormatException] {
      deserializer.deserializeKey("notAnInt_false", null)
    }
  }

  it should "throw IllegalArgumentException for a non-boolean internal flag" in {
    intercept[IllegalArgumentException] {
      deserializer.deserializeKey("0_notABool", null)
    }
  }

  it should "throw NumberFormatException when a separator-less key is non-numeric" in {
    intercept[NumberFormatException] {
      deserializer.deserializeKey("missingSeparator", null)
    }
  }

  it should "throw NumberFormatException for an empty key" in {
    intercept[NumberFormatException] {
      deserializer.deserializeKey("", null)
    }
  }

  it should "throw ArrayIndexOutOfBoundsException when only the id is provided" in {
    intercept[ArrayIndexOutOfBoundsException] {
      deserializer.deserializeKey("5", null)
    }
  }

  it should "ignore extra trailing underscore-separated segments" in {
    deserializer.deserializeKey("1_true_extra", null) shouldBe PortIdentity(1, internal = true)
  }

  it should "eventually reject keys with extra trailing segments (pendingUntilFixed)" in pendingUntilFixed {
    // Documented contract: a `PortIdentityKeySerializer` output is exactly
    // `id_internal` — two underscore-separated segments. Anything else is
    // corrupt JSON and should be rejected, not silently truncated. The
    // current implementation is lenient (see characterization test above);
    // this pendingUntilFixed flips to passing once the parser is hardened,
    // then `pendingUntilFixed` inverts that into a deliberate failure forcing
    // the marker to be removed.
    intercept[IllegalArgumentException] {
      deserializer.deserializeKey("1_true_extra", null)
    }
  }
}
