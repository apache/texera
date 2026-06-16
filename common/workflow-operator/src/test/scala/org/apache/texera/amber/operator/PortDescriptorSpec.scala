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

package org.apache.texera.amber.operator

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.texera.amber.core.workflow.{HashPartition, UnknownPartition}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class PortDescriptorSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  // The trait is mixed-in by Jackson-deserialized descriptor classes; for
  // contract testing, mix it into a plain test class. The variables are
  // `var` and default to `null` by design (Jackson sets them after
  // construction).
  private class TestPortHolder extends PortDescriptor

  private def desc(id: String): PortDescription =
    PortDescription(
      portID = id,
      displayName = s"display-$id",
      disallowMultiInputs = false,
      isDynamicPort = false,
      partitionRequirement = UnknownPartition()
    )

  // ---------------------------------------------------------------------------
  // PortDescriptor — defaults + mutability
  // ---------------------------------------------------------------------------

  "PortDescriptor (default)" should
    "leave inputPorts and outputPorts as null (Jackson sets them post-construction)" in {
    val holder = new TestPortHolder
    assert(holder.inputPorts == null)
    assert(holder.outputPorts == null)
  }

  "PortDescriptor" should "allow inputPorts to be assigned post-construction" in {
    val holder = new TestPortHolder
    holder.inputPorts = List(desc("a"))
    assert(holder.inputPorts.size == 1)
    assert(holder.inputPorts.head.portID == "a")
  }

  it should "allow outputPorts to be assigned post-construction" in {
    val holder = new TestPortHolder
    holder.outputPorts = List(desc("o-1"), desc("o-2"))
    assert(holder.outputPorts.size == 2)
    assert(holder.outputPorts.map(_.portID) == List("o-1", "o-2"))
  }

  // ---------------------------------------------------------------------------
  // PortDescription — case-class constructor + defaults
  // ---------------------------------------------------------------------------

  "PortDescription" should "preserve every field passed to the constructor" in {
    val p = PortDescription(
      portID = "p",
      displayName = "Display",
      disallowMultiInputs = true,
      isDynamicPort = true,
      partitionRequirement = UnknownPartition(),
      dependencies = List(0, 1)
    )
    assert(p.portID == "p")
    assert(p.displayName == "Display")
    assert(p.disallowMultiInputs)
    assert(p.isDynamicPort)
    assert(p.partitionRequirement == UnknownPartition())
    assert(p.dependencies == List(0, 1))
  }

  it should "default `dependencies` to `List.empty` when omitted" in {
    val p = PortDescription(
      portID = "p",
      displayName = "d",
      disallowMultiInputs = false,
      isDynamicPort = false,
      partitionRequirement = UnknownPartition()
    )
    assert(p.dependencies == List.empty[Int])
  }

  // ---------------------------------------------------------------------------
  // PortDescription — equality / copy (case-class semantics)
  // ---------------------------------------------------------------------------

  "PortDescription equality" should "compare every field" in {
    val a = PortDescription("p", "d", false, false, UnknownPartition())
    val b = PortDescription("p", "d", false, false, UnknownPartition())
    val c = PortDescription("OTHER", "d", false, false, UnknownPartition())
    assert(a == b)
    assert(a.hashCode == b.hashCode)
    assert(a != c)
  }

  "PortDescription.copy" should "replace only the field supplied" in {
    val base = PortDescription("p", "d", false, false, UnknownPartition())
    val updated = base.copy(displayName = "new")
    assert(updated.portID == "p")
    assert(updated.displayName == "new")
    // Original is unchanged — immutable case-class semantics.
    assert(base.displayName == "d")
  }

  // ---------------------------------------------------------------------------
  // Backward-compat shim — `@JsonIgnoreProperties("allowMultiInputs")` on
  // PortDescription (workflows persisted before PR #4379 emitted the key
  // `allowMultiInputs`; Jackson must NOT fail on unknown field).
  // ---------------------------------------------------------------------------

  "PortDescription" should
    "carry @JsonIgnoreProperties(\"allowMultiInputs\") (backward-compat for pre-#4379 workflows)" in {
    val ann = classOf[PortDescription].getAnnotation(classOf[JsonIgnoreProperties])
    assert(ann != null, "@JsonIgnoreProperties must be present on PortDescription")
    assert(
      ann.value.toSet.contains("allowMultiInputs"),
      s"expected 'allowMultiInputs' in @JsonIgnoreProperties value, got: ${ann.value.toList}"
    )
  }

  it should
    "deserialize a JSON payload that includes the legacy `allowMultiInputs` key without failing" in {
    // The legacy-key tolerance is what we pin here; the `type: "none"`
    // discriminator on `partitionRequirement` is `PartitionInfo`'s
    // `@JsonTypeInfo` shape for `UnknownPartition`. If
    // `allowMultiInputs` were missing from `@JsonIgnoreProperties`, the
    // default Jackson behavior would raise `UnrecognizedPropertyException`
    // here and the case would fail.
    val legacyJson = """
      {
        "portID": "p",
        "displayName": "d",
        "allowMultiInputs": true,
        "disallowMultiInputs": false,
        "isDynamicPort": false,
        "partitionRequirement": { "type": "none" }
      }
    """
    val parsed = scala.util.Try(objectMapper.readValue(legacyJson, classOf[PortDescription]))
    assert(parsed.isSuccess, s"legacy JSON must deserialize without error; got: $parsed")
  }

  it should
    "deserialize a truly-legacy payload that has `allowMultiInputs` but NO `disallowMultiInputs`" in {
    // Pre-#4379 workflow JSONs (e.g. `bin/single-node/examples/workflows/`)
    // emit ONLY the old `allowMultiInputs` key, not the new
    // `disallowMultiInputs`. `PortDescription` does NOT declare a Scala
    // constructor default for `disallowMultiInputs`, so what we're
    // pinning is Jackson's missing-boolean deserialization behavior
    // (the project uses `jackson-module-no-ctor-deser`, which yields
    // `false` for a missing primitive boolean) — not a Scala default.
    // The mixed-legacy case above does not exercise this since both
    // keys are present.
    val trulyLegacyJson = """
      {
        "portID": "p",
        "displayName": "d",
        "allowMultiInputs": true,
        "isDynamicPort": false,
        "partitionRequirement": { "type": "none" }
      }
    """
    val parsed =
      scala.util.Try(objectMapper.readValue(trulyLegacyJson, classOf[PortDescription]))
    assert(parsed.isSuccess, s"truly-legacy JSON must deserialize without error; got: $parsed")
    val pd = parsed.get
    assert(pd.portID == "p")
    // Jackson's missing-boolean behavior: `disallowMultiInputs` is
    // omitted from the JSON and ends up `false` after deserialization.
    assert(!pd.disallowMultiInputs)
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip — every field survives serde
  // ---------------------------------------------------------------------------

  "PortDescription JSON round-trip" should
    "preserve every field including the polymorphic partitionRequirement" in {
    val p = PortDescription(
      portID = "rt-1",
      displayName = "rt-display",
      disallowMultiInputs = true,
      isDynamicPort = true,
      partitionRequirement = UnknownPartition(),
      dependencies = List(2, 3, 5)
    )
    val json = objectMapper.writeValueAsString(p)
    val restored = objectMapper.readValue(json, classOf[PortDescription])
    assert(restored.portID == p.portID)
    assert(restored.displayName == p.displayName)
    assert(restored.disallowMultiInputs == p.disallowMultiInputs)
    assert(restored.isDynamicPort == p.isDynamicPort)
    assert(restored.dependencies == p.dependencies)
    // partitionRequirement is a sealed `PartitionInfo` carrying a
    // `@JsonTypeInfo` discriminator (`type` property). Drift in the
    // annotation set or in the subtype-name table would silently
    // break workflow JSON deserialization; pin this field too.
    assert(restored.partitionRequirement == p.partitionRequirement)
  }

  it should "round-trip a non-Unknown PartitionInfo (HashPartition with attributes)" in {
    // Vary the PartitionInfo subtype away from the default so the
    // discriminator + value layout is exercised end-to-end.
    val p = PortDescription(
      portID = "rt-2",
      displayName = "hash",
      disallowMultiInputs = false,
      isDynamicPort = false,
      partitionRequirement = HashPartition(List("a", "b")),
      dependencies = List.empty
    )
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(p), classOf[PortDescription])
    assert(restored.partitionRequirement == HashPartition(List("a", "b")))
  }
}
