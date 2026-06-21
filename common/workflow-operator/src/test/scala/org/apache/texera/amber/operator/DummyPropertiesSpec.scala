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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class DummyPropertiesSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Defaults
  // ---------------------------------------------------------------------------

  "DummyProperties" should "default dummyProperty and dummyValue to the empty string" in {
    val d = new DummyProperties
    assert(d.dummyProperty == "")
    assert(d.dummyValue == "")
  }

  // ---------------------------------------------------------------------------
  // Mutability
  // ---------------------------------------------------------------------------

  it should "allow both fields to be assigned post-construction" in {
    val d = new DummyProperties
    d.dummyProperty = "p"
    d.dummyValue = "v"
    assert(d.dummyProperty == "p")
    assert(d.dummyValue == "v")
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip
  // ---------------------------------------------------------------------------

  "DummyProperties JSON round-trip" should "preserve both fields" in {
    val d = new DummyProperties
    d.dummyProperty = "hello"
    d.dummyValue = "world"
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(d),
      classOf[DummyProperties]
    )
    assert(restored.dummyProperty == "hello")
    assert(restored.dummyValue == "world")
  }

  it should "round-trip default (empty) values cleanly" in {
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(new DummyProperties),
      classOf[DummyProperties]
    )
    assert(restored.dummyProperty == "")
    assert(restored.dummyValue == "")
  }

  // ---------------------------------------------------------------------------
  // Annotations
  // ---------------------------------------------------------------------------

  "DummyProperties#dummyProperty" should "carry @JsonProperty" in {
    val jp = classOf[DummyProperties]
      .getDeclaredField("dummyProperty")
      .getAnnotation(classOf[JsonProperty])
    assert(jp != null)
  }

  "DummyProperties#dummyValue" should "carry @JsonProperty" in {
    val jp = classOf[DummyProperties]
      .getDeclaredField("dummyValue")
      .getAnnotation(classOf[JsonProperty])
    assert(jp != null)
  }

  // ---------------------------------------------------------------------------
  // Instance independence
  // ---------------------------------------------------------------------------

  it should "construct two independent instances (no static state shared)" in {
    val a = new DummyProperties
    val b = new DummyProperties
    a.dummyProperty = "first"
    assert(b.dummyProperty == "")
  }
}
