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

import org.apache.texera.amber.operator.visualization.ChartConfigSpecSupport
import org.scalatest.flatspec.AnyFlatSpec

class HierarchySectionSpec extends AnyFlatSpec with ChartConfigSpecSupport {

  "HierarchySection" should "default attributeName to the empty string" in {
    assert(new HierarchySection().attributeName == "")
  }

  it should "allow attributeName to be reassigned post-construction" in {
    val s = new HierarchySection
    s.attributeName = "country"
    assert(s.attributeName == "country")
  }

  "HierarchySection JSON round-trip" should "preserve attributeName" in {
    val s = new HierarchySection
    s.attributeName = "region"
    assert(checkRoundTrip(s, classOf[HierarchySection]).attributeName == "region")
  }

  it should "round-trip default (empty) values cleanly" in {
    assert(checkRoundTrip(new HierarchySection, classOf[HierarchySection]).attributeName == "")
  }

  "HierarchySection#attributeName" should "carry @JsonProperty(required = true)" in {
    assertAttrJsonPropertyRequired(classOf[HierarchySection])
  }

  it should "carry @AutofillAttributeName" in {
    assertAttrAutofill(classOf[HierarchySection], "@AutofillAttributeName expected")
  }

  it should "carry @NotNull with the canonical error message" in {
    assertAttrNotNull(classOf[HierarchySection])
  }

  it should "construct two independent instances (no static state shared)" in {
    val a = new HierarchySection
    val b = new HierarchySection
    a.attributeName = "first"
    assert(b.attributeName == "")
  }
}
