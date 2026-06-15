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
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GlobalPortIdentitySerdeSpec extends AnyFlatSpec with Matchers {

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
    restored shouldBe original
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
      restored shouldBe p
    }
  }

  it should "produce the documented format for default and non-default values" in {
    // Pin the exact format. If this changes, callers reading existing
    // VFS URIs from disk will break — locking it down forces a deliberate
    // migration story.
    globalPort().serializeAsString shouldBe
      "(logicalOpId=op-A,layerName=main,portId=0,isInternal=false,isInput=true)"
    globalPort(
      logical = "op-Z",
      layer = "extra-layer",
      portIdValue = 7,
      internal = true,
      input = false
    ).serializeAsString shouldBe
      "(logicalOpId=op-Z,layerName=extra-layer,portId=7,isInternal=true,isInput=false)"
  }

  it should "round-trip identifiers containing dashes and dots (regex non-comma matcher)" in {
    // The deserialization regex uses `[^,]+` for the field body, so any
    // non-comma character is fair game. Cover the realistic counter-
    // examples (dashes, dots) since logical op ids and layer names use
    // both; if the regex were ever tightened to alphanumerics only, this
    // would fail on purpose.
    val p = globalPort(logical = "my.op-with-dashes.v2", layer = "main-1")
    GlobalPortIdentitySerde.deserializeFromString(p.serializeAsString) shouldBe p
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
    // `String.toBoolean` is strict: only "true" / "false" (case-insensitive)
    // pass; anything else throws IllegalArgumentException.
    val malformed = "(logicalOpId=op-A,layerName=main,portId=0,isInternal=maybe,isInput=true)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(malformed)
    }
  }

  it should "throw IllegalArgumentException on a completely malformed string" in {
    val ex = intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString("not even close")
    }
    ex.getMessage should include("not even close")
  }

  it should "use no underscore in its own format characters (separators / keys)" in {
    // Pin the format-character invariant: the wrapping `(...)`, the field
    // separators `,`, the key=value separators, and the field NAMES
    // themselves contain no underscore. Verify by stripping the input
    // field values, so anything left in the output is purely from
    // `serializeAsString`'s own format.
    val s = globalPort(logical = "x", layer = "x").serializeAsString
    val formatChars = s.replace("x", "").replace("0", "").replace("false", "").replace("true", "")
    formatChars should not include "_"
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
  // Edge cases
  // ---------------------------------------------------------------------------

  it should "accept underscores on deserialize even though serialize rejects them (serialize/deserialize asymmetry)" in {
    // The deserializer regex `[^,]+` does not reject underscores, so a
    // hand-crafted string with underscored logicalOpId / layerName parses
    // fine — unlike the serializer, which rejects them at the boundary.
    // Characterize this asymmetry so a future tightening of the regex
    // breaks this test deliberately.
    val s = "(logicalOpId=op_A,layerName=main_layer,portId=0,isInternal=false,isInput=true)"
    val restored = GlobalPortIdentitySerde.deserializeFromString(s)
    restored.opId.logicalOpId.id shouldBe "op_A"
    restored.opId.layerName shouldBe "main_layer"
  }

  it should "fail to round-trip a logicalOpId containing a comma (separators are not escaped)" in {
    // The format does not escape its `,` separator, so a logicalOpId with
    // an embedded comma serializes into a string the deserializer can no
    // longer parse back into the same value.
    val s = globalPort(logical = "op,A").serializeAsString
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(s)
    }
  }

  it should "round-trip Int.MaxValue but reject an out-of-Int-range portId on deserialize" in {
    // Upper boundary of the portId domain round-trips intact.
    val p = globalPort(portIdValue = Int.MaxValue)
    GlobalPortIdentitySerde.deserializeFromString(p.serializeAsString) shouldBe p
    // A value past Int range fails in `.toInt` with NumberFormatException.
    val overflow =
      "(logicalOpId=op-A,layerName=main,portId=9999999999,isInternal=false,isInput=true)"
    intercept[NumberFormatException] {
      GlobalPortIdentitySerde.deserializeFromString(overflow)
    }
  }

  it should "accept mixed-case boolean fields on deserialize (String.toBoolean is case-insensitive)" in {
    // `String.toBoolean` accepts any case variant of true/false, so a
    // tampered/legacy string with `True` / `FALSE` still parses.
    val s = "(logicalOpId=op-A,layerName=main,portId=0,isInternal=True,isInput=FALSE)"
    val restored = GlobalPortIdentitySerde.deserializeFromString(s)
    restored.portId.internal shouldBe true
    restored.input shouldBe false
  }

  it should "round-trip a value containing '=' (only ',' is a sensitive separator)" in {
    // Counterpart to the comma case above: the deserializer anchors on the
    // literal `logicalOpId=` prefix and then captures up to the next comma
    // (`[^,]+`), so an embedded `=` is harmless and round-trips intact.
    // Pin this so a future regex change that special-cased `=` would fail.
    val p = globalPort(logical = "a=b")
    GlobalPortIdentitySerde.deserializeFromString(p.serializeAsString) shouldBe p
  }

  it should "serialize an empty logicalOpId but fail to deserialize the result (serialize does not guard emptiness)" in {
    // The serializer only rejects underscores and negative portIds, not empty
    // identifiers, so an empty logicalOpId serializes into `(logicalOpId=,...)`.
    // The deserializer's `[^,]+` requires at least one character, so that
    // output can no longer be parsed back — the serialize side of the same
    // asymmetry the "empty field body" deserialize test characterizes.
    val s = globalPort(logical = "").serializeAsString
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(s)
    }
  }

  it should "name the offending field in the require failure message" in {
    // The three serialize-side guards throw IllegalArgumentException; assert
    // the message identifies which field failed so a regression that wires a
    // check to the wrong field (e.g. validating logicalOpId in layerName's
    // guard) is caught instead of silently still throwing.
    intercept[IllegalArgumentException] {
      globalPort(logical = "__x").serializeAsString
    }.getMessage should include("logicalOpId")
    intercept[IllegalArgumentException] {
      globalPort(layer = "a_b").serializeAsString
    }.getMessage should include("layerName")
    intercept[IllegalArgumentException] {
      globalPort(portIdValue = -1).serializeAsString
    }.getMessage should include("portId")
  }
}
