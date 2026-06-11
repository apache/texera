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

import org.apache.texera.amber.core.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.scalatest.flatspec.AnyFlatSpec

class PortIdentitySerdeSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // GlobalPortIdentitySerde
  // ---------------------------------------------------------------------------

  private def globalPort(
      logical: String = "op-A",
      layer: String = "main",
      portIdValue: Int = 0,
      internal: Boolean = false,
      input: Boolean = true
  ): GlobalPortIdentity =
    GlobalPortIdentity(
      opId = PhysicalOpIdentity(OperatorIdentity(logical), layer),
      portId = PortIdentity(id = portIdValue, internal = internal),
      input = input
    )

  "GlobalPortIdentitySerde" should "round-trip a default GlobalPortIdentity through serializeAsString → deserializeFromString" in {
    val original = globalPort()
    val restored = GlobalPortIdentitySerde.deserializeFromString(original.serializeAsString)
    assert(restored == original)
  }

  it should "preserve all five fields independently across the round-trip" in {
    // Vary each field individually so a regression that swapped two fields
    // (e.g., isInput / isInternal) would surface here, not as a general
    // round-trip failure.
    val cases = Seq(
      globalPort(logical = "op-A"),
      globalPort(logical = "op-Z"),
      globalPort(layer = "main"),
      globalPort(layer = "extra-layer"),
      globalPort(portIdValue = 0),
      globalPort(portIdValue = 7),
      globalPort(internal = false),
      globalPort(internal = true),
      globalPort(input = true),
      globalPort(input = false)
    )
    cases.foreach { p =>
      val s = p.serializeAsString
      val restored = GlobalPortIdentitySerde.deserializeFromString(s)
      assert(restored == p, s"round-trip mismatch for $p (serialized: $s)")
    }
  }

  it should "produce the documented format for default and non-default values" in {
    // Pin the exact format. If this changes, callers reading existing
    // VFS URIs from disk will break — locking it down forces a deliberate
    // migration story.
    assert(
      globalPort().serializeAsString ==
        "(logicalOpId=op-A,layerName=main,portId=0,isInternal=false,isInput=true)"
    )
    assert(
      globalPort(
        logical = "op-Z",
        layer = "extra-layer",
        portIdValue = 7,
        internal = true,
        input = false
      ).serializeAsString ==
        "(logicalOpId=op-Z,layerName=extra-layer,portId=7,isInternal=true,isInput=false)"
    )
  }

  it should "round-trip identifiers containing dashes and dots (regex non-comma matcher)" in {
    // The deserialization regex uses `[^,]+` for the field body, so any
    // non-comma character is fair game. Cover the realistic counter-
    // examples (dashes, dots) since logical op ids and layer names use
    // both; if the regex were ever tightened to alphanumerics only, this
    // would fail on purpose.
    val p = globalPort(logical = "my.op-with-dashes.v2", layer = "main-1")
    assert(GlobalPortIdentitySerde.deserializeFromString(p.serializeAsString) == p)
  }

  it should "throw IllegalArgumentException when serializing a negative port id" in {
    // Port ids are array indices and must be non-negative; the serializer
    // rejects negatives so corrupt data can't reach VFS URIs.
    intercept[IllegalArgumentException] {
      globalPort(portIdValue = -1).serializeAsString
    }
  }

  it should "throw IllegalArgumentException when deserializing a negative port id" in {
    // Symmetric: a hand-crafted string with a negative portId must be
    // rejected by the deserializer too (so tampered URIs don't slip
    // through).
    val malformed = "(logicalOpId=op-A,layerName=main,portId=-1,isInternal=false,isInput=true)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(malformed)
    }
  }

  it should "throw IllegalArgumentException when the input has the wrong field order" in {
    // The regex pins the documented field order; a swapped order should
    // not silently parse with confused values.
    val swapped = "(layerName=main,logicalOpId=op-A,portId=0,isInternal=false,isInput=true)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(swapped)
    }
  }

  it should "throw IllegalArgumentException when the input has trailing content past the closing paren" in {
    val withTrailing =
      "(logicalOpId=op-A,layerName=main,portId=0,isInternal=false,isInput=true) extra"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(withTrailing)
    }
  }

  it should "throw IllegalArgumentException when a field body is empty" in {
    // `[^,]+` requires at least one character, so an empty layerName
    // (`,layerName=,`) must fail to match.
    val emptyLayer = "(logicalOpId=op-A,layerName=,portId=0,isInternal=false,isInput=true)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(emptyLayer)
    }
  }

  it should "throw IllegalArgumentException on a completely malformed string" in {
    val ex = intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString("not even close")
    }
    assert(ex.getMessage.contains("not even close"))
  }

  it should "throw IllegalArgumentException when a required field is missing" in {
    // Drop isInput.
    val malformed = "(logicalOpId=op-A,layerName=main,portId=0,isInternal=false)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(malformed)
    }
  }

  it should "throw NumberFormatException when portId is non-numeric" in {
    // The regex matches (`[^,]+`) but `.toInt` fails. NumberFormatException
    // extends IllegalArgumentException; assert the more specific type so a
    // regression that swallowed/rewrapped it is visible.
    val malformed = "(logicalOpId=op-A,layerName=main,portId=NaN,isInternal=false,isInput=true)"
    intercept[NumberFormatException] {
      GlobalPortIdentitySerde.deserializeFromString(malformed)
    }
  }

  it should "throw IllegalArgumentException when a boolean field is non-boolean" in {
    // `String.toBoolean` is strict: only \"true\" / \"false\" (case-insensitive)
    // pass; anything else throws IllegalArgumentException.
    val malformed = "(logicalOpId=op-A,layerName=main,portId=0,isInternal=maybe,isInput=true)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(malformed)
    }
  }

  it should "use no underscore in its own format characters (separators / keys)" in {
    // Pin the format-character invariant: the wrapping `(...)`, the field
    // separators `,`, the key=value separators, and the field NAMES
    // themselves contain no underscore. Verify by building the format with
    // empty-string-replacement values for every input field, so anything
    // left in the output is purely from `serializeAsString`'s own format.
    // (For the layerName field the empty-input variant is rejected by the
    // deserializer regex; here we only check the SERIALIZED output, not the
    // round-trip.)
    val s = globalPort(logical = "x", layer = "x").serializeAsString
    val formatChars = s.replace("x", "").replace("0", "").replace("false", "").replace("true", "")
    assert(!formatChars.contains("_"), s"format characters must be underscore-free: $formatChars")
  }

  it should "throw IllegalArgumentException when logicalOpId contains an underscore" in {
    // Enforces the documented VFS-compatibility contract: the serialized
    // form must be underscore-free. The serializer rejects underscored
    // inputs at the boundary instead of silently emitting a string that
    // would interfere with VFS URI parsing downstream.
    intercept[IllegalArgumentException] {
      globalPort(logical = "__DummyOperator").serializeAsString
    }
  }

  it should "throw IllegalArgumentException when layerName contains an underscore" in {
    // Both fields enforce the same invariant; cover them independently so
    // a partial fix that only validates one surfaces as a test failure.
    intercept[IllegalArgumentException] {
      globalPort(layer = "main_source_0_op").serializeAsString
    }
  }

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
