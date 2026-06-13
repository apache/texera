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

package org.apache.texera.amber.operator.visualization.dumbbellPlot

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import javax.validation.constraints.NotNull

class DumbbellDotConfigSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Default state
  // ---------------------------------------------------------------------------

  "DumbbellDotConfig" should "default dotValue to the empty string" in {
    val c = new DumbbellDotConfig
    assert(c.dotValue == "")
  }

  // ---------------------------------------------------------------------------
  // Mutability
  // ---------------------------------------------------------------------------

  it should "allow dotValue to be assigned post-construction" in {
    val c = new DumbbellDotConfig
    c.dotValue = "numeric-col"
    assert(c.dotValue == "numeric-col")
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip — verify both the Scala field name AND the wire key
  // ---------------------------------------------------------------------------

  "DumbbellDotConfig JSON round-trip" should
    "preserve dotValue via the wire-key `dot` (per @JsonProperty(value = \"dot\"))" in {
    val original = new DumbbellDotConfig
    original.dotValue = "amount"
    val json = objectMapper.writeValueAsString(original)
    // Parse the JSON into a tree and assert on field presence + value
    // directly — this stays robust to formatting changes (spaces, key
    // ordering) that pure substring matching would mistake for drift.
    val tree = objectMapper.readTree(json)
    assert(tree.has("dot"), s"expected wire-key 'dot' in JSON, got: $json")
    assert(tree.get("dot").asText() == "amount")
    assert(
      !tree.has("dotValue"),
      s"field name 'dotValue' must NOT appear as a JSON key, got: $json"
    )
    val restored = objectMapper.readValue(json, classOf[DumbbellDotConfig])
    assert(restored.dotValue == "amount")
  }

  it should "deserialize from the wire-key `dot` back into dotValue" in {
    val json = """{"dot":"amount"}"""
    val restored = objectMapper.readValue(json, classOf[DumbbellDotConfig])
    assert(restored.dotValue == "amount")
  }

  // ---------------------------------------------------------------------------
  // Annotations
  // ---------------------------------------------------------------------------

  "DumbbellDotConfig#dotValue" should
    "carry @JsonProperty(value = 'dot', required = true)" in {
    val jp = classOf[DumbbellDotConfig]
      .getDeclaredField("dotValue")
      .getAnnotation(classOf[JsonProperty])
    assert(jp != null)
    val actualValue = jp.value
    assert(actualValue == "dot", s"expected value='dot', got: '$actualValue'")
    assert(jp.required, "dotValue must be marked required")
  }

  it should "carry @NotNull (javax.validation contract)" in {
    val notNull = classOf[DumbbellDotConfig]
      .getDeclaredField("dotValue")
      .getAnnotation(classOf[NotNull])
    assert(notNull != null, "dotValue must carry @NotNull for javax.validation")
  }

  it should "carry @AutofillAttributeName (UI populates the dropdown from the input schema)" in {
    val ann = classOf[DumbbellDotConfig]
      .getDeclaredField("dotValue")
      .getAnnotation(classOf[AutofillAttributeName])
    assert(ann != null)
  }

  "DumbbellDotConfig (class-level)" should
    "carry @JsonSchemaInject restricting `dot` to integer/long/double attribute types" in {
    // The class-level @JsonSchemaInject is what tells the UI to filter the
    // attribute dropdown to numeric columns only. Drift here would silently
    // accept non-numeric attributes and break the chart at render time.
    val ann = classOf[DumbbellDotConfig].getAnnotation(classOf[JsonSchemaInject])
    assert(ann != null, "class-level @JsonSchemaInject must be present")
    val payload = ann.json
    assert(payload.contains("attributeTypeRules"))
    assert(payload.contains("\"dot\""))
    assert(payload.contains("integer"))
    assert(payload.contains("long"))
    assert(payload.contains("double"))
  }

  // ---------------------------------------------------------------------------
  // Instance independence
  // ---------------------------------------------------------------------------

  it should "construct two independent instances (no static state shared)" in {
    val a = new DumbbellDotConfig
    val b = new DumbbellDotConfig
    a.dotValue = "first"
    assert(b.dotValue == "")
  }
}
