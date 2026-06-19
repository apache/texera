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

package org.apache.texera.workflow

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.exc.{MismatchedInputException, ValueInstantiationException}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class LogicalLinkSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Primary constructor + case-class semantics
  // ---------------------------------------------------------------------------

  "LogicalLink primary constructor" should "expose the four fields it was constructed with" in {
    val link = LogicalLink(
      fromOpId = OperatorIdentity("op-A"),
      fromPortId = PortIdentity(0),
      toOpId = OperatorIdentity("op-B"),
      toPortId = PortIdentity(1, internal = true)
    )
    assert(link.fromOpId == OperatorIdentity("op-A"))
    assert(link.fromPortId == PortIdentity(0))
    assert(link.toOpId == OperatorIdentity("op-B"))
    assert(link.toPortId == PortIdentity(1, internal = true))
  }

  "LogicalLink case-class equality" should "use structural equality across all four fields" in {
    val a =
      LogicalLink(OperatorIdentity("x"), PortIdentity(0), OperatorIdentity("y"), PortIdentity(1))
    val b =
      LogicalLink(OperatorIdentity("x"), PortIdentity(0), OperatorIdentity("y"), PortIdentity(1))
    assert(a == b)
    assert(a.hashCode == b.hashCode)
  }

  it should "distinguish links that differ only in fromOpId" in {
    val a =
      LogicalLink(OperatorIdentity("x"), PortIdentity(0), OperatorIdentity("y"), PortIdentity(1))
    val b =
      LogicalLink(OperatorIdentity("z"), PortIdentity(0), OperatorIdentity("y"), PortIdentity(1))
    assert(a != b)
  }

  it should "distinguish links that differ only in toPortId.internal" in {
    val a = LogicalLink(
      OperatorIdentity("x"),
      PortIdentity(0),
      OperatorIdentity("y"),
      PortIdentity(1, internal = false)
    )
    val b = LogicalLink(
      OperatorIdentity("x"),
      PortIdentity(0),
      OperatorIdentity("y"),
      PortIdentity(1, internal = true)
    )
    assert(a != b)
  }

  it should "reject a self-loop link (fromOpId == toOpId) regardless of port" in {
    // The constructor rejects fromOpId == toOpId — a workflow edge whose
    // source and sink are the same operator can never be schedulable, so
    // we fail fast here rather than letting it travel through the planner.
    val ex = intercept[IllegalArgumentException] {
      LogicalLink(
        OperatorIdentity("op-A"),
        PortIdentity(0),
        OperatorIdentity("op-A"),
        PortIdentity(1)
      )
    }
    assert(ex.getMessage.contains("self-loop"))
  }

  it should "reject a null fromOpId / toOpId in the primary constructor" in {
    intercept[IllegalArgumentException] {
      LogicalLink(null, PortIdentity(0), OperatorIdentity("op-B"), PortIdentity(1))
    }
    intercept[IllegalArgumentException] {
      LogicalLink(OperatorIdentity("op-A"), PortIdentity(0), null, PortIdentity(1))
    }
  }

  it should "reject an OperatorIdentity wrapping a null id in the primary constructor" in {
    intercept[IllegalArgumentException] {
      LogicalLink(
        OperatorIdentity(null),
        PortIdentity(0),
        OperatorIdentity("op-B"),
        PortIdentity(1)
      )
    }
    intercept[IllegalArgumentException] {
      LogicalLink(
        OperatorIdentity("op-A"),
        PortIdentity(0),
        OperatorIdentity(null),
        PortIdentity(1)
      )
    }
  }

  it should "reject an OperatorIdentity wrapping an empty id in the primary constructor" in {
    intercept[IllegalArgumentException] {
      LogicalLink(OperatorIdentity(""), PortIdentity(0), OperatorIdentity("op-B"), PortIdentity(1))
    }
    intercept[IllegalArgumentException] {
      LogicalLink(OperatorIdentity("op-A"), PortIdentity(0), OperatorIdentity(""), PortIdentity(1))
    }
  }

  // ---------------------------------------------------------------------------
  // Secondary @JsonCreator constructor (string opId variant)
  // ---------------------------------------------------------------------------

  "LogicalLink secondary @JsonCreator constructor" should "wrap raw String op ids in OperatorIdentity" in {
    val link = new LogicalLink(
      fromOpId = "op-A",
      fromPortId = PortIdentity(0),
      toOpId = "op-B",
      toPortId = PortIdentity(1)
    )
    assert(link.fromOpId == OperatorIdentity("op-A"))
    assert(link.toOpId == OperatorIdentity("op-B"))
    // Equal to a link built via the primary constructor.
    assert(
      link == LogicalLink(
        OperatorIdentity("op-A"),
        PortIdentity(0),
        OperatorIdentity("op-B"),
        PortIdentity(1)
      )
    )
  }

  it should "accept identifiers containing dashes / dots / digits (no normalization)" in {
    val link = new LogicalLink("my.op-1", PortIdentity(0), "my.op-2", PortIdentity(1))
    assert(link.fromOpId == OperatorIdentity("my.op-1"))
    assert(link.toOpId == OperatorIdentity("my.op-2"))
  }

  it should "reject the empty string as an op id via the @JsonCreator constructor" in {
    intercept[IllegalArgumentException] {
      new LogicalLink("", PortIdentity(0), "op-B", PortIdentity(1))
    }
    intercept[IllegalArgumentException] {
      new LogicalLink("op-A", PortIdentity(0), "", PortIdentity(1))
    }
  }

  it should "reject a null string op id via the @JsonCreator constructor" in {
    intercept[IllegalArgumentException] {
      new LogicalLink(null: String, PortIdentity(0), "op-B", PortIdentity(1))
    }
    intercept[IllegalArgumentException] {
      new LogicalLink("op-A", PortIdentity(0), null: String, PortIdentity(1))
    }
  }

  it should "reject a self-loop via the @JsonCreator constructor (same string op id)" in {
    val ex = intercept[IllegalArgumentException] {
      new LogicalLink("op-A", PortIdentity(0), "op-A", PortIdentity(1))
    }
    assert(ex.getMessage.contains("self-loop"))
  }

  // ---------------------------------------------------------------------------
  // Jackson round-trip (production objectMapper)
  // ---------------------------------------------------------------------------
  //
  // These tests use the same `JSONUtils.objectMapper` that production uses
  // to read user-saved workflow JSON, so a regression in the Jackson
  // wiring (annotations, default-Scala-module config) surfaces here.

  "LogicalLink Jackson deserialization" should
    "deserialize fromOpId / toOpId from raw String values via the secondary @JsonCreator constructor" in {
    // Build the JSON by hand to mimic a user-saved workflow file where
    // `fromOpId` and `toOpId` are written as plain strings (the only shape
    // production actually receives, since the frontend emits them as
    // strings). Jackson dispatches to the @JsonCreator string-overload
    // constructor.
    val node = objectMapper.createObjectNode()
    node.put("fromOpId", "op-A")
    node.set("fromPortId", objectMapper.valueToTree[JsonNode](PortIdentity(0)))
    node.put("toOpId", "op-B")
    node.set("toPortId", objectMapper.valueToTree[JsonNode](PortIdentity(1)))
    val link = objectMapper.treeToValue(node, classOf[LogicalLink])
    assert(link.fromOpId == OperatorIdentity("op-A"))
    assert(link.toOpId == OperatorIdentity("op-B"))
    assert(link.fromPortId == PortIdentity(0))
    assert(link.toPortId == PortIdentity(1))
  }

  it should "emit `fromOpId` / `toOpId` JSON keys pinned by @JsonProperty annotations" in {
    // Only `fromOpId` / `toOpId` carry `@JsonProperty` in `LogicalLink`;
    // a Scala-side rename of either parameter would still keep the
    // JSON key stable, which is the saved-workflow contract these
    // annotations pin.
    val link = LogicalLink(
      OperatorIdentity("op-A"),
      PortIdentity(0),
      OperatorIdentity("op-B"),
      PortIdentity(1)
    )
    val tree = objectMapper.valueToTree[JsonNode](link)
    assert(tree.has("fromOpId"))
    assert(tree.has("toOpId"))
  }

  it should "emit `fromPortId` / `toPortId` JSON keys derived from Scala parameter names (no @JsonProperty)" in {
    // Pin: the port-id JSON keys come from Scala parameter names since
    // there is no `@JsonProperty` annotation on those fields. A
    // parameter rename WOULD silently break saved-workflow compatibility
    // for these keys — pin so a future rename without an accompanying
    // `@JsonProperty` annotation breaks this on purpose.
    val link = LogicalLink(
      OperatorIdentity("op-A"),
      PortIdentity(0),
      OperatorIdentity("op-B"),
      PortIdentity(1)
    )
    val tree = objectMapper.valueToTree[JsonNode](link)
    assert(tree.has("fromPortId"))
    assert(tree.has("toPortId"))
  }

  it should "NOT round-trip through writeValueAsString (the @JsonCreator string overload is incompatible with the object-shape OperatorIdentity that writeValueAsString emits)" in {
    // Characterization of a real asymmetry tracked by
    // https://github.com/apache/texera/issues/5042. Production reads
    // user-saved workflow JSON where `fromOpId`/`toOpId` are plain
    // strings, but `objectMapper.writeValueAsString` writes
    // OperatorIdentity as `{"id":"op-A"}` (the case-class object form).
    // Re-reading the emitted JSON fails because Jackson dispatches on the
    // @JsonCreator string overload, which can't accept an object for
    // fromOpId. When the issue is fixed (additional @JsonCreator object
    // overload or a custom @JsonDeserialize), this test must flip to a
    // passing round-trip assertion alongside the fix.
    val original = LogicalLink(
      OperatorIdentity("op-A"),
      PortIdentity(0),
      OperatorIdentity("op-B"),
      PortIdentity(1)
    )
    val json = objectMapper.writeValueAsString(original)
    // Parse the emitted JSON and confirm the structural shape — fromOpId
    // is an object with an `id` field of "op-A". Avoids depending on
    // exact key ordering or escaping.
    val tree = objectMapper.readTree(json)
    assert(tree.path("fromOpId").isObject, s"expected fromOpId to be an object: $json")
    assert(tree.path("fromOpId").path("id").asText() == "op-A")
    // Re-reading the just-emitted JSON fails because the @JsonCreator
    // String overload can't accept the object-shape fromOpId.
    intercept[MismatchedInputException] {
      objectMapper.readValue(json, classOf[LogicalLink])
    }
  }

  it should "reject missing string op-id fields when deserializing via Jackson" in {
    // When `fromOpId` / `toOpId` are omitted, Jackson invokes the
    // @JsonCreator with `null` for the missing String args. The primary
    // constructor's `require` on non-null/non-empty ids then throws, and
    // Jackson wraps it in `ValueInstantiationException` with the original
    // `IllegalArgumentException` as the cause.
    val empty = objectMapper.createObjectNode()
    val ex = intercept[ValueInstantiationException] {
      objectMapper.treeToValue(empty, classOf[LogicalLink])
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
  }
}
