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

package org.apache.texera.amber.operator.visualization.tablesChart

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import javax.validation.constraints.NotNull

class TablesConfigSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Default state
  // ---------------------------------------------------------------------------

  "TablesConfig" should "default attributeName to the empty string" in {
    val c = new TablesConfig
    assert(c.attributeName == "")
  }

  // ---------------------------------------------------------------------------
  // Mutability — the @JsonProperty bag is `var`-based by design
  // ---------------------------------------------------------------------------

  it should "allow attributeName to be assigned post-construction" in {
    val c = new TablesConfig
    c.attributeName = "col-a"
    assert(c.attributeName == "col-a")
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip preserves the field
  // ---------------------------------------------------------------------------

  "TablesConfig JSON round-trip" should
    "serialize and deserialize attributeName unchanged" in {
    val original = new TablesConfig
    original.attributeName = "my-attr"
    val json = objectMapper.writeValueAsString(original)
    val restored = objectMapper.readValue(json, classOf[TablesConfig])
    assert(restored.attributeName == "my-attr")
  }

  it should "round-trip the default empty attributeName" in {
    val original = new TablesConfig
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(original),
      classOf[TablesConfig]
    )
    assert(restored.attributeName == "")
  }

  // ---------------------------------------------------------------------------
  // Independent instances — no static-field leakage
  // ---------------------------------------------------------------------------

  it should "construct two independent instances (no static state shared)" in {
    val a = new TablesConfig
    val b = new TablesConfig
    a.attributeName = "first"
    assert(b.attributeName == "", "second instance must not see first instance's mutation")
  }

  // ---------------------------------------------------------------------------
  // Annotations — verified via reflection (the Jackson + validation
  // contract is what consumers actually depend on)
  // ---------------------------------------------------------------------------

  "TablesConfig#attributeName" should "carry @JsonProperty(required = true)" in {
    val field = classOf[TablesConfig].getDeclaredField("attributeName")
    val jp = field.getAnnotation(classOf[JsonProperty])
    assert(jp != null, "attributeName must carry @JsonProperty")
    assert(jp.required, "attributeName must be marked required")
  }

  it should "carry @NotNull (javax.validation contract)" in {
    val field = classOf[TablesConfig].getDeclaredField("attributeName")
    val notNull = field.getAnnotation(classOf[NotNull])
    assert(notNull != null, "attributeName must carry @NotNull for javax.validation")
  }

  it should
    "carry @AutofillAttributeName (UI populates the dropdown from the input schema)" in {
    val ann = classOf[TablesConfig]
      .getDeclaredField("attributeName")
      .getAnnotation(classOf[AutofillAttributeName])
    assert(ann != null, "@AutofillAttributeName must be present so the UI auto-populates")
  }
}
