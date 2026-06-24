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

package org.apache.texera.amber.compiler.model

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.exc.ValueInstantiationException
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Unit tests for the workflow-compiling-service [[LogicalLink]].
  *
  * Unlike the amber engine's LogicalLink, this version is intentionally
  * lenient: it carries no `require` guards so that the compiler can
  * represent partially-built or invalid workflows during editing without
  * throwing. Tests here pin that contract and verify the Jackson wiring
  * that lets the service round-trip saved workflow JSON.
  */
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

  // ---------------------------------------------------------------------------
  // Secondary String constructor
  // ---------------------------------------------------------------------------

  "LogicalLink secondary String constructor" should "wrap raw String op ids in OperatorIdentity" in {
    val link = new LogicalLink(
      fromOpId = "op-A",
      fromPortId = PortIdentity(0),
      toOpId = "op-B",
      toPortId = PortIdentity(1)
    )
    assert(link.fromOpId == OperatorIdentity("op-A"))
    assert(link.toOpId == OperatorIdentity("op-B"))
    assert(
      link == LogicalLink(
        OperatorIdentity("op-A"),
        PortIdentity(0),
        OperatorIdentity("op-B"),
        PortIdentity(1)
      )
    )
  }

  it should "accept identifiers containing dashes, dots, and digits" in {
    val link = new LogicalLink("my.op-1", PortIdentity(0), "my.op-2", PortIdentity(1))
    assert(link.fromOpId == OperatorIdentity("my.op-1"))
    assert(link.toOpId == OperatorIdentity("my.op-2"))
  }

  // ---------------------------------------------------------------------------
  // Leniency contract: no require guards in the compiler-service variant
  // ---------------------------------------------------------------------------
  //
  // The compiler-service LogicalLink is intentionally lenient so that a
  // mid-edit, partially-built workflow (e.g. one where an operator id has
  // not yet been assigned) can be represented without throwing. The amber
  // engine's LogicalLink enforces strict validation; tests for that live in
  // amber/src/test.

  "LogicalLink (compiler-service)" should "accept a null OperatorIdentity id without throwing" in {
    val link = LogicalLink(
      OperatorIdentity(null),
      PortIdentity(0),
      OperatorIdentity("op-B"),
      PortIdentity(1)
    )
    assert(link.fromOpId == OperatorIdentity(null))
  }

  it should "accept a self-loop link (fromOpId == toOpId) without throwing" in {
    val link = LogicalLink(
      OperatorIdentity("op-A"),
      PortIdentity(0),
      OperatorIdentity("op-A"),
      PortIdentity(1)
    )
    assert(link.fromOpId == link.toOpId)
  }

  // ---------------------------------------------------------------------------
  // Jackson round-trip (production objectMapper)
  // ---------------------------------------------------------------------------
  //
  // These tests use the same `JSONUtils.objectMapper` that production uses
  // to read user-saved workflow JSON, so a regression in the Jackson wiring
  // (annotations, default-Scala-module config) surfaces here.

  "LogicalLink Jackson deserialization" should
    "deserialize fromOpId / toOpId from raw String values via the Jackson creator" in {
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

  it should "round-trip through writeValueAsString when OperatorIdentity fields use object shape" in {
    val original = LogicalLink(
      OperatorIdentity("op-A"),
      PortIdentity(0),
      OperatorIdentity("op-B"),
      PortIdentity(1)
    )
    val json = objectMapper.writeValueAsString(original)
    val tree = objectMapper.readTree(json)
    assert(tree.path("fromOpId").isObject, s"expected fromOpId to be an object: $json")
    assert(tree.path("fromOpId").path("id").asText() == "op-A")

    val roundTripped = objectMapper.readValue(json, classOf[LogicalLink])
    assert(roundTripped == original)
  }

  it should "emit `fromOpId` / `toOpId` JSON keys pinned by @JsonProperty annotations" in {
    // @JsonProperty pins the key name — a Scala parameter rename keeps the
    // JSON key stable, which is required for saved-workflow compatibility.
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
    // No @JsonProperty on these fields — the JSON key comes from the Scala
    // parameter name. A future rename WOULD silently break saved-workflow
    // compatibility; this test exists so that rename breaks here on purpose.
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

  it should "produce OperatorIdentity(null) when fromOpId is absent from JSON entirely (lenient)" in {
    // When fromOpId / toOpId are omitted entirely, Jackson passes null to the
    // @JsonCreator parameter. readOperatorIdentity treats null as OperatorIdentity(null)
    // rather than throwing — lenient by design for partial workflows mid-edit.
    val empty = objectMapper.createObjectNode()
    val link = objectMapper.treeToValue(empty, classOf[LogicalLink])
    assert(link.fromOpId == OperatorIdentity(null))
    assert(link.toOpId == OperatorIdentity(null))
  }

  it should "produce OperatorIdentity(null) for an object-shape id with no id field (lenient)" in {
    // WCS LogicalLink is lenient: an object-shape fromOpId with no "id" field
    // produces OperatorIdentity(null) rather than throwing. This lets the
    // compiler represent partially-built workflows mid-edit.
    val node = objectMapper.createObjectNode()
    node.set("fromOpId", objectMapper.createObjectNode()) // {} — no "id" field
    node.set("fromPortId", objectMapper.valueToTree[JsonNode](PortIdentity(0)))
    node.put("toOpId", "op-B")
    node.set("toPortId", objectMapper.valueToTree[JsonNode](PortIdentity(1)))

    val link = objectMapper.treeToValue(node, classOf[LogicalLink])
    assert(link.fromOpId == OperatorIdentity(null))
  }

  it should "wrap IllegalArgumentException in ValueInstantiationException when an opId is a numeric value instead of text or object" in {
    // A number is not a valid shape for fromOpId regardless of leniency —
    // readOperatorIdentity explicitly throws for non-text, non-object nodes.
    val node = objectMapper.createObjectNode()
    node.put("fromOpId", 12345)
    node.set("fromPortId", objectMapper.valueToTree[JsonNode](PortIdentity(0)))
    node.put("toOpId", "op-B")
    node.set("toPortId", objectMapper.valueToTree[JsonNode](PortIdentity(1)))

    val ex = intercept[ValueInstantiationException] {
      objectMapper.treeToValue(node, classOf[LogicalLink])
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(ex.getCause.getMessage.contains("fromOpId must be a string or an object"))
  }

  it should "throw when an object-shape opId has a non-textual `id` field" in {
    // Leniency covers null/empty/self-loop semantics, not malformed JSON
    // types: `{"id": 123}` is rejected rather than coerced to "123".
    val node = objectMapper.createObjectNode()
    val badOpId = objectMapper.createObjectNode()
    badOpId.put("id", 123)
    node.set("fromOpId", badOpId)
    node.set("fromPortId", objectMapper.valueToTree[JsonNode](PortIdentity(0)))
    node.put("toOpId", "op-B")
    node.set("toPortId", objectMapper.valueToTree[JsonNode](PortIdentity(1)))
    val ex = intercept[ValueInstantiationException] {
      objectMapper.treeToValue(node, classOf[LogicalLink])
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(ex.getCause.getMessage.contains("fromOpId.id must be a string"))
  }

  it should "treat an explicit JSON null op id as OperatorIdentity(null) (exercises the node.isNull branch)" in {
    // An explicit `"fromOpId": null` arrives as a NullNode (an absent field
    // arrives as Java null); WCS leniency maps it to OperatorIdentity(null).
    val node = objectMapper.createObjectNode()
    node.set("fromOpId", objectMapper.nullNode())
    node.set("fromPortId", objectMapper.valueToTree[JsonNode](PortIdentity(0)))
    node.put("toOpId", "op-B")
    node.set("toPortId", objectMapper.valueToTree[JsonNode](PortIdentity(1)))
    val link = objectMapper.treeToValue(node, classOf[LogicalLink])
    assert(link.fromOpId == OperatorIdentity(null))
  }

  it should "treat an object-shape op id with explicit null `id` as OperatorIdentity(null) (exercises the idNode.isNull branch)" in {
    // `{"id": null}` — `id` is present but JSON null, so idNode is a NullNode.
    // WCS leniency maps this to OperatorIdentity(null).
    val opId = objectMapper.createObjectNode()
    opId.set("id", objectMapper.nullNode())
    val node = objectMapper.createObjectNode()
    node.set("fromOpId", opId)
    node.set("fromPortId", objectMapper.valueToTree[JsonNode](PortIdentity(0)))
    node.put("toOpId", "op-B")
    node.set("toPortId", objectMapper.valueToTree[JsonNode](PortIdentity(1)))
    val link = objectMapper.treeToValue(node, classOf[LogicalLink])
    assert(link.fromOpId == OperatorIdentity(null))
  }
}
