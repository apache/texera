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

package org.apache.texera.amber.operator.visualization.continuousErrorBands

import com.fasterxml.jackson.annotation.JsonProperty
import javax.validation.constraints.NotNull
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.visualization.lineChart.LineConfig
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class BandConfigSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Inheritance
  // ---------------------------------------------------------------------------

  "BandConfig" should "extend LineConfig (compile-time enforced)" in {
    val lc: LineConfig = new BandConfig
    assert(lc != null)
  }

  // ---------------------------------------------------------------------------
  // Defaults
  // ---------------------------------------------------------------------------

  it should "default yUpper, yLower, and fillColor to the empty string" in {
    val c = new BandConfig
    assert(c.yUpper == "")
    assert(c.yLower == "")
    assert(c.fillColor == "")
  }

  // ---------------------------------------------------------------------------
  // Mutability
  // ---------------------------------------------------------------------------

  it should "allow all three fields to be reassigned post-construction" in {
    val c = new BandConfig
    c.yUpper = "upper"
    c.yLower = "lower"
    c.fillColor = "#ff0000"
    assert(c.yUpper == "upper")
    assert(c.yLower == "lower")
    assert(c.fillColor == "#ff0000")
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip
  // ---------------------------------------------------------------------------

  "BandConfig JSON round-trip" should "preserve yUpper, yLower, and fillColor" in {
    val c = new BandConfig
    c.yUpper = "u"
    c.yLower = "l"
    c.fillColor = "rgba(0,0,255,0.5)"
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(c),
      classOf[BandConfig]
    )
    assert(restored.yUpper == "u")
    assert(restored.yLower == "l")
    assert(restored.fillColor == "rgba(0,0,255,0.5)")
  }

  // ---------------------------------------------------------------------------
  // Annotations — required=true on yUpper/yLower, required=false on fillColor
  // ---------------------------------------------------------------------------

  "BandConfig#yUpper" should "carry @JsonProperty(required = true) + @NotNull + @AutofillAttributeName" in {
    val cls = classOf[BandConfig]
    val jp = cls.getDeclaredField("yUpper").getAnnotation(classOf[JsonProperty])
    assert(jp != null)
    assert(jp.required)
    val notNull = cls.getDeclaredField("yUpper").getAnnotation(classOf[NotNull])
    assert(notNull != null)
    assert(notNull.message == "Y-Axis Upper Bound cannot be empty")
    val autofill = cls.getDeclaredField("yUpper").getAnnotation(classOf[AutofillAttributeName])
    assert(autofill != null)
  }

  "BandConfig#yLower" should "carry @JsonProperty(required = true) + @NotNull + @AutofillAttributeName" in {
    val cls = classOf[BandConfig]
    val jp = cls.getDeclaredField("yLower").getAnnotation(classOf[JsonProperty])
    assert(jp != null)
    assert(jp.required)
    val notNull = cls.getDeclaredField("yLower").getAnnotation(classOf[NotNull])
    assert(notNull != null)
    assert(notNull.message == "Y-Axis Lower Bound cannot be empty")
    val autofill = cls.getDeclaredField("yLower").getAnnotation(classOf[AutofillAttributeName])
    assert(autofill != null)
  }

  "BandConfig#fillColor" should "carry @JsonProperty(required = false) and no @NotNull" in {
    val cls = classOf[BandConfig]
    val jp = cls.getDeclaredField("fillColor").getAnnotation(classOf[JsonProperty])
    assert(jp != null)
    assert(!jp.required)
    val notNull = cls.getDeclaredField("fillColor").getAnnotation(classOf[NotNull])
    assert(notNull == null)
  }

  // ---------------------------------------------------------------------------
  // Instance independence
  // ---------------------------------------------------------------------------

  it should "construct two independent instances (no static state shared)" in {
    val a = new BandConfig
    val b = new BandConfig
    a.yUpper = "mutated"
    assert(b.yUpper == "")
  }
}
