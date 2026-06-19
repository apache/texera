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

class PortIdentitySerdeSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // PortIdentityKeySerializer.portIdToString (companion, not the Jackson class)
  // ---------------------------------------------------------------------------

  "PortIdentityKeySerializer.portIdToString" should "format a PortIdentity as `id_internal`" in {
    assert(PortIdentityKeySerializer.portIdToString(PortIdentity(0, internal = false)) == "0_false")
    assert(PortIdentityKeySerializer.portIdToString(PortIdentity(7, internal = true)) == "7_true")
  }

  // ---------------------------------------------------------------------------
  // PortIdentityKeySerializer + PortIdentityKeyDeserializer (Jackson wiring)
  // ---------------------------------------------------------------------------
  //
  // These tests use the production `JSONUtils.objectMapper` directly so a
  // regression in the singleton wiring (e.g. the module that registers the
  // PortIdentity key (de)serializer being removed or reordered) surfaces
  // here, not just on a freshly-constructed mapper.

  "PortIdentity Jackson key (de)serialization" should "round-trip a Map[PortIdentity, String] via JSONUtils.objectMapper" in {
    val original = Map(
      PortIdentity(0, internal = false) -> "a",
      PortIdentity(1, internal = true) -> "b"
    )
    val json = objectMapper.writeValueAsString(original)
    // Verify the JSON keys match the documented `id_internal` format.
    assert(json.contains("\"0_false\""))
    assert(json.contains("\"1_true\""))
    val tref = objectMapper.getTypeFactory
      .constructMapType(classOf[java.util.HashMap[_, _]], classOf[PortIdentity], classOf[String])
    val restored: java.util.Map[PortIdentity, String] = objectMapper.readValue(json, tref)
    import scala.jdk.CollectionConverters._
    assert(restored.asScala.toMap == original)
  }

  it should "round-trip an empty Map[PortIdentity, V] without invoking the (de)serializer" in {
    val original = Map.empty[PortIdentity, String]
    val json = objectMapper.writeValueAsString(original)
    val tref = objectMapper.getTypeFactory
      .constructMapType(classOf[java.util.HashMap[_, _]], classOf[PortIdentity], classOf[String])
    val restored: java.util.Map[PortIdentity, String] = objectMapper.readValue(json, tref)
    assert(restored.isEmpty)
  }

  "PortIdentityKeyDeserializer.deserializeKey" should "throw NumberFormatException for a non-integer id" in {
    val d = new PortIdentityKeyDeserializer
    intercept[NumberFormatException] {
      d.deserializeKey("notAnInt_false", null)
    }
  }

  it should "throw IllegalArgumentException for a non-boolean internal flag" in {
    val d = new PortIdentityKeyDeserializer
    intercept[IllegalArgumentException] {
      d.deserializeKey("0_notABool", null)
    }
  }

  it should "throw NumberFormatException when the underscore separator is missing and the whole string is non-numeric" in {
    // `key.split("_")` on a separator-less non-numeric string yields a
    // single-element array, and `parts(0).toInt` fires first → NFE.
    val d = new PortIdentityKeyDeserializer
    intercept[NumberFormatException] {
      d.deserializeKey("missingSeparator", null)
    }
  }

  it should "throw ArrayIndexOutOfBoundsException when only the id is provided (no `_internal` suffix)" in {
    // Different separator-missing path: `\"5\".split(\"_\")` yields
    // [\"5\"], parts(0).toInt = 5 succeeds, then parts(1) reads past the
    // end. Pin this failure mode explicitly so a future safer parser
    // breaks the spec on purpose (and the safer error type is chosen
    // consciously).
    val d = new PortIdentityKeyDeserializer
    intercept[ArrayIndexOutOfBoundsException] {
      d.deserializeKey("5", null)
    }
  }

  it should "silently accept extra trailing underscore-separated segments (lenient parser, current behavior)" in {
    // Pin the current lenient behavior: `parts(0).toInt` and
    // `parts(1).toBoolean` ignore everything past `parts(1)`, so a key
    // like `"1_true_garbage"` deserializes to `PortIdentity(1, true)`
    // without complaint. The strict-rejection variant lives in a
    // pendingUntilFixed test below; characterizing today's lenient
    // path here means a future-tightening fix would need to update
    // both tests deliberately.
    val d = new PortIdentityKeyDeserializer
    val pid = d.deserializeKey("1_true_garbage", null)
    assert(pid == PortIdentity(1, internal = true))
  }

  it should "eventually reject keys with extra trailing segments (pendingUntilFixed)" in pendingUntilFixed {
    // Documented contract: a `PortIdentityKeySerializer` output is exactly
    // `id_internal` — two underscore-separated segments. Anything else is
    // corrupt JSON and should be rejected, not silently truncated. The
    // current implementation is lenient (see characterization test
    // above); this pendingUntilFixed flips to passing once the parser
    // is hardened, then `pendingUntilFixed` inverts that into a
    // deliberate failure forcing the marker to be removed.
    val d = new PortIdentityKeyDeserializer
    intercept[IllegalArgumentException] {
      d.deserializeKey("1_true_garbage", null)
    }
  }
}
