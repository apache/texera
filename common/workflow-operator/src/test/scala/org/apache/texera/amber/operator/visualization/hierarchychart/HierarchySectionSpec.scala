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

package org.apache.texera.amber.operator.visualization.hierarchychart

import com.fasterxml.jackson.annotation.JsonProperty
import javax.validation.constraints.NotNull
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class HierarchySectionSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Defaults
  // ---------------------------------------------------------------------------

  "HierarchySection" should "default attributeName to the empty string" in {
    val s = new HierarchySection
    assert(s.attributeName == "")
  }

  // ---------------------------------------------------------------------------
  // Mutability
  // ---------------------------------------------------------------------------

  it should "allow attributeName to be reassigned post-construction" in {
    val s = new HierarchySection
    s.attributeName = "country"
    assert(s.attributeName == "country")
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip
  // ---------------------------------------------------------------------------

  "HierarchySection JSON round-trip" should "preserve attributeName" in {
    val s = new HierarchySection
    s.attributeName = "region"
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(s),
      classOf[HierarchySection]
    )
    assert(restored.attributeName == "region")
  }

  it should "round-trip default (empty) values cleanly" in {
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(new HierarchySection),
      classOf[HierarchySection]
    )
    assert(restored.attributeName == "")
  }

  // ---------------------------------------------------------------------------
  // Annotations — required=true + @AutofillAttributeName + @NotNull
  // ---------------------------------------------------------------------------

  "HierarchySection#attributeName" should "carry @JsonProperty(required = true)" in {
    val jp = classOf[HierarchySection]
      .getDeclaredField("attributeName")
      .getAnnotation(classOf[JsonProperty])
    assert(jp != null)
    assert(jp.required)
  }

  it should "carry @AutofillAttributeName" in {
    val ann = classOf[HierarchySection]
      .getDeclaredField("attributeName")
      .getAnnotation(classOf[AutofillAttributeName])
    assert(ann != null)
  }

  it should "carry @NotNull with the canonical error message" in {
    val ann = classOf[HierarchySection]
      .getDeclaredField("attributeName")
      .getAnnotation(classOf[NotNull])
    assert(ann != null)
    assert(ann.message == "Attribute Name cannot be empty")
  }

  // ---------------------------------------------------------------------------
  // Instance independence
  // ---------------------------------------------------------------------------

  it should "construct two independent instances (no static state shared)" in {
    val a = new HierarchySection
    val b = new HierarchySection
    a.attributeName = "first"
    assert(b.attributeName == "")
  }
}
