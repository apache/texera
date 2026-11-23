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

package org.apache.texera.amber.operator.visualization.figureFactoryTable

import org.apache.texera.amber.operator.visualization.ChartConfigSpecSupport
import org.scalatest.flatspec.AnyFlatSpec

class FigureFactoryTableConfigSpec extends AnyFlatSpec with ChartConfigSpecSupport {

  "FigureFactoryTableConfig" should "default attributeName to the empty string" in {
    assert(new FigureFactoryTableConfig().attributeName == "")
  }

  it should "allow attributeName to be assigned post-construction" in {
    val c = new FigureFactoryTableConfig
    c.attributeName = "col-z"
    assert(c.attributeName == "col-z")
  }

  "FigureFactoryTableConfig JSON round-trip" should "preserve attributeName" in {
    val original = new FigureFactoryTableConfig
    original.attributeName = "x"
    assert(checkRoundTrip(original, classOf[FigureFactoryTableConfig]).attributeName == "x")
  }

  it should "round-trip the default empty attributeName" in {
    assert(
      checkRoundTrip(
        new FigureFactoryTableConfig,
        classOf[FigureFactoryTableConfig]
      ).attributeName == ""
    )
  }

  "FigureFactoryTableConfig#attributeName" should "carry @JsonProperty(required = true)" in {
    assertAttrJsonPropertyRequired(classOf[FigureFactoryTableConfig])
  }

  it should "carry @AutofillAttributeName (UI relies on this to populate a dropdown)" in {
    assertAttrAutofill(
      classOf[FigureFactoryTableConfig],
      "@AutofillAttributeName must be present so the UI auto-populates"
    )
  }

  it should "construct two independent instances (no static state shared)" in {
    val a = new FigureFactoryTableConfig
    val b = new FigureFactoryTableConfig
    a.attributeName = "first"
    assert(b.attributeName == "")
  }
}
