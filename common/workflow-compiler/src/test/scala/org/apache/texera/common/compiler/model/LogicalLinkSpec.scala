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

package org.apache.texera.common.compiler.model

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.exc.ValueInstantiationException
import com.fasterxml.jackson.databind.node.{IntNode, TextNode}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class LogicalLinkSpec extends AnyFlatSpec {

  /** A fresh link from op-A port 0 to op-B port 1. */
  private def abLink: LogicalLink =
    LogicalLink(
      OperatorIdentity("op-A"),
      PortIdentity(0),
      OperatorIdentity("op-B"),
      PortIdentity(1)
    )

  /** The primary constructor rejects `badId` at either end of a link. By name, so each
    * identity is built inside its own intercept.
    */
  private def assertRejectedAtEitherEnd(badId: => OperatorIdentity): Unit = {
    intercept[IllegalArgumentException] {
      LogicalLink(badId, PortIdentity(0), OperatorIdentity("op-B"), PortIdentity(1))
    }
    intercept[IllegalArgumentException] {
      LogicalLink(OperatorIdentity("op-A"), PortIdentity(0), badId, PortIdentity(1))
    }
  }

  /** The secondary String constructor rejects `badId` at either end of a link. */
  private def assertRejectedStringAtEitherEnd(badId: String): Unit = {
    intercept[IllegalArgumentException] {
      new LogicalLink(badId, PortIdentity(0), "op-B", PortIdentity(1))
    }
    intercept[IllegalArgumentException] {
      new LogicalLink("op-A", PortIdentity(0), badId, PortIdentity(1))
    }
  }

  /** A saved link from `fromOpId` to op-B, read through the production mapper. */
  private def deserializeLink(fromOpId: JsonNode): LogicalLink = {
    val node = objectMapper.createObjectNode()
    node.set("fromOpId", fromOpId)
    node.set("fromPortId", objectMapper.valueToTree[JsonNode](PortIdentity(0)))
    node.put("toOpId", "op-B")
    node.set("toPortId", objectMapper.valueToTree[JsonNode](PortIdentity(1)))
    objectMapper.treeToValue(node, classOf[LogicalLink])
  }

  /** The IllegalArgumentException the creator throws for `fromOpId`, as Jackson wraps it. */
  private def rejectionOf(fromOpId: JsonNode): Throwable = {
    val ex = intercept[ValueInstantiationException](deserializeLink(fromOpId))
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    ex.getCause
  }

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
    assertRejectedAtEitherEnd(null)
  }

  it should "reject an OperatorIdentity wrapping a null id in the primary constructor" in {
    assertRejectedAtEitherEnd(OperatorIdentity(null))
  }

  it should "reject an OperatorIdentity wrapping an empty id in the primary constructor" in {
    assertRejectedAtEitherEnd(OperatorIdentity(""))
  }

  // ---------------------------------------------------------------------------
  // Secondary string opId constructor
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
    // Equal to a link built via the primary constructor.
    assert(link == abLink)
  }

  it should "accept identifiers containing dashes / dots / digits (no normalization)" in {
    val link = new LogicalLink("my.op-1", PortIdentity(0), "my.op-2", PortIdentity(1))
    assert(link.fromOpId == OperatorIdentity("my.op-1"))
    assert(link.toOpId == OperatorIdentity("my.op-2"))
  }

  it should "reject the empty string as an op id via the secondary String constructor" in {
    assertRejectedStringAtEitherEnd("")
  }

  it should "reject a null string op id via the secondary String constructor" in {
    assertRejectedStringAtEitherEnd(null)
  }

  it should "reject a self-loop via the secondary String constructor (same string op id)" in {
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
    "deserialize fromOpId / toOpId from raw String values via the Jackson creator" in {
    // Build the JSON by hand to mimic a user-saved workflow file where
    // `fromOpId` and `toOpId` are written as plain strings (the only shape
    // production actually receives, since the frontend emits them as
    // strings). Jackson dispatches to the @JsonCreator constructor.
    val link = deserializeLink(TextNode.valueOf("op-A"))
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
    val tree = objectMapper.valueToTree[JsonNode](abLink)
    assert(tree.has("fromOpId"))
    assert(tree.has("toOpId"))
  }

  it should "emit `fromPortId` / `toPortId` JSON keys derived from Scala parameter names (no @JsonProperty)" in {
    // Pin: the port-id JSON keys come from Scala parameter names since
    // there is no `@JsonProperty` annotation on those fields. A
    // parameter rename WOULD silently break saved-workflow compatibility
    // for these keys — pin so a future rename without an accompanying
    // `@JsonProperty` annotation breaks this on purpose.
    val tree = objectMapper.valueToTree[JsonNode](abLink)
    assert(tree.has("fromPortId"))
    assert(tree.has("toPortId"))
  }

  it should "round-trip through writeValueAsString when OperatorIdentity fields use object shape" in {
    val original = abLink
    val json = objectMapper.writeValueAsString(original)
    // Parse the emitted JSON and confirm the structural shape — fromOpId
    // is an object with an `id` field of "op-A". Avoids depending on
    // exact key ordering or escaping.
    val tree = objectMapper.readTree(json)
    assert(tree.path("fromOpId").isObject, s"expected fromOpId to be an object: $json")
    assert(tree.path("fromOpId").path("id").asText() == "op-A")

    val roundTripped = objectMapper.readValue(json, classOf[LogicalLink])
    assert(roundTripped == original)
  }

  it should "reject missing op-id fields when deserializing via Jackson" in {
    // When `fromOpId` / `toOpId` are omitted, Jackson invokes the
    // @JsonCreator with `null` for the missing args. The primary
    // constructor's `require` on non-null/non-empty ids then throws, and
    // Jackson wraps it in `ValueInstantiationException` with the original
    // `IllegalArgumentException` as the cause.
    val empty = objectMapper.createObjectNode()
    val ex = intercept[ValueInstantiationException] {
      objectMapper.treeToValue(empty, classOf[LogicalLink])
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
  }

  it should "reject an object-shape op id with no `id` field (null id fails the require guard)" in {
    // An object-shape fromOpId lacking an `id` field resolves to
    // OperatorIdentity(null), which the primary constructor's `require`
    // rejects — surfaced as ValueInstantiationException.
    val noId = objectMapper.createObjectNode() // {} — no "id"
    assert(rejectionOf(noId).getMessage.contains("fromOpId must be non-null"))
  }

  it should "reject an object-shape op id whose `id` field is non-textual" in {
    // `{"id": 123}` is malformed: `id` must be a string (or null). The
    // helper throws rather than silently coercing 123 -> "123"; Jackson
    // wraps the IllegalArgumentException in a ValueInstantiationException.
    val badOpId = objectMapper.createObjectNode()
    badOpId.put("id", 123)
    assert(rejectionOf(badOpId).getMessage.contains("fromOpId.id must be a string"))
  }

  it should "reject an op id that is neither a string nor an object (e.g. a number)" in {
    // A top-level numeric fromOpId hits the final else branch of
    // readOperatorIdentity, which throws; Jackson wraps it.
    val number = IntNode.valueOf(12345)
    assert(rejectionOf(number).getMessage.contains("fromOpId must be a string or an object"))
  }

  it should "reject an explicit JSON null op id (exercises the node.isNull branch)" in {
    // An explicit `"fromOpId": null` arrives as a NullNode (an absent field
    // arrives as Java null), exercising the `node.isNull` branch; require
    // then rejects the null id.
    rejectionOf(objectMapper.nullNode())
  }

  it should "reject an object-shape op id with an explicit null `id` (exercises the idNode.isNull branch)" in {
    // `{"id": null}` makes idNode a NullNode, exercising the `idNode.isNull`
    // branch; require then rejects the resulting OperatorIdentity(null).
    val opId = objectMapper.createObjectNode()
    opId.set("id", objectMapper.nullNode())
    rejectionOf(opId)
  }
}
