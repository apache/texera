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

import org.apache.texera.amber.operator.visualization.ChartConfigSpecSupport
import org.scalatest.flatspec.AnyFlatSpec

class TablesConfigSpec extends AnyFlatSpec with ChartConfigSpecSupport {

  "TablesConfig" should "default attributeName to the empty string" in {
    assert(new TablesConfig().attributeName == "")
  }

  it should "allow attributeName to be assigned post-construction" in {
    val c = new TablesConfig
    c.attributeName = "col-a"
    assert(c.attributeName == "col-a")
  }

  "TablesConfig JSON round-trip" should "serialize and deserialize attributeName unchanged" in {
    val original = new TablesConfig
    original.attributeName = "my-attr"
    assert(checkRoundTrip(original, classOf[TablesConfig]).attributeName == "my-attr")
  }

  it should "round-trip the default empty attributeName" in {
    assert(checkRoundTrip(new TablesConfig, classOf[TablesConfig]).attributeName == "")
  }

  it should "construct two independent instances (no static state shared)" in {
    val a = new TablesConfig
    val b = new TablesConfig
    a.attributeName = "first"
    assert(b.attributeName == "", "second instance must not see first instance's mutation")
  }

  "TablesConfig#attributeName" should "carry @JsonProperty(required = true)" in {
    assertAttrJsonPropertyRequired(classOf[TablesConfig])
  }

  it should "carry @NotNull (javax.validation contract)" in {
    assertAttrNotNull(classOf[TablesConfig])
  }

  it should "carry @AutofillAttributeName (UI populates the dropdown from the input schema)" in {
    assertAttrAutofill(
      classOf[TablesConfig],
      "@AutofillAttributeName must be present so the UI auto-populates"
    )
  }
}
