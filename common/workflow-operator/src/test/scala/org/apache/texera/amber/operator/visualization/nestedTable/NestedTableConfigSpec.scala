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

package org.apache.texera.amber.operator.visualization.nestedTable

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class NestedTableConfigSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Default state
  // ---------------------------------------------------------------------------

  "NestedTableConfig" should "default every field to the empty string" in {
    val c = new NestedTableConfig
    assert(c.attributeGroup == "")
    assert(c.originalName == "")
    assert(c.newName == "")
  }

  // ---------------------------------------------------------------------------
  // Mutability
  // ---------------------------------------------------------------------------

  it should "allow every field to be assigned post-construction" in {
    val c = new NestedTableConfig
    c.attributeGroup = "g"
    c.originalName = "orig"
    c.newName = "renamed"
    assert(c.attributeGroup == "g")
    assert(c.originalName == "orig")
    assert(c.newName == "renamed")
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip
  // ---------------------------------------------------------------------------

  "NestedTableConfig JSON round-trip" should "preserve all three fields" in {
    val original = new NestedTableConfig
    original.attributeGroup = "group-1"
    original.originalName = "src-name"
    original.newName = "dst-name"
    val json = objectMapper.writeValueAsString(original)
    val restored = objectMapper.readValue(json, classOf[NestedTableConfig])
    assert(restored.attributeGroup == "group-1")
    assert(restored.originalName == "src-name")
    assert(restored.newName == "dst-name")
  }

  it should
    "serialize newName under the JSON key `name` (per @JsonProperty(value = \"name\"))" in {
    // The wire-key for `newName` is `name`, not `newName` — a regression
    // that drifted to `newName` would silently break every workflow JSON
    // that carries this config. Parse the JSON into a tree so the
    // assertion is robust to Jackson formatting (spaces, key ordering)
    // and unambiguous about which key carries which value.
    val c = new NestedTableConfig
    c.newName = "renamed"
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(c))
    assert(tree.has("name"), s"expected wire-key 'name' in JSON tree: $tree")
    assert(tree.get("name").asText() == "renamed")
    assert(
      !tree.has("newName"),
      s"field name 'newName' must NOT appear as a JSON key, got: $tree"
    )
  }

  it should "deserialize from the wire-key `name` back into newName" in {
    val json = """{"attributeGroup":"g","originalName":"orig","name":"renamed"}"""
    val restored = objectMapper.readValue(json, classOf[NestedTableConfig])
    assert(restored.newName == "renamed")
  }

  // ---------------------------------------------------------------------------
  // Annotations — required/optional via reflection
  // ---------------------------------------------------------------------------

  "NestedTableConfig#attributeGroup" should "carry @JsonProperty(required = true)" in {
    val jp = classOf[NestedTableConfig]
      .getDeclaredField("attributeGroup")
      .getAnnotation(classOf[JsonProperty])
    assert(jp != null && jp.required)
  }

  "NestedTableConfig#originalName" should "carry @JsonProperty(required = true)" in {
    val jp = classOf[NestedTableConfig]
      .getDeclaredField("originalName")
      .getAnnotation(classOf[JsonProperty])
    assert(jp != null && jp.required)
  }

  "NestedTableConfig#newName" should
    "carry @JsonProperty(value = 'name', required = false) — the optional renamed field" in {
    val jp = classOf[NestedTableConfig]
      .getDeclaredField("newName")
      .getAnnotation(classOf[JsonProperty])
    assert(jp != null)
    val actualValue = jp.value
    assert(actualValue == "name", s"expected value='name', got: '$actualValue'")
    assert(!jp.required, "newName must NOT be marked required")
  }

  // ---------------------------------------------------------------------------
  // Instance independence
  // ---------------------------------------------------------------------------

  it should "construct two independent instances (no static state shared)" in {
    val a = new NestedTableConfig
    val b = new NestedTableConfig
    a.attributeGroup = "first"
    assert(b.attributeGroup == "")
  }
}
