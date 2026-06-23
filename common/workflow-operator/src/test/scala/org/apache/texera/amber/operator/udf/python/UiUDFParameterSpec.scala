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

package org.apache.texera.amber.operator.udf.python

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UiUDFParameterSpec extends AnyFlatSpec with Matchers {

  "UiUDFParameter" should "default value to the empty string" in {
    // `attribute` is an uninitialized `var` (null) on a fresh instance; only
    // `value` has a default and is safe to read.
    (new UiUDFParameter).value shouldBe ""
  }

  it should "round-trip attribute and value through JSON" in {
    val p = new UiUDFParameter
    p.attribute = new Attribute("col", AttributeType.STRING)
    p.value = "x"
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(p), classOf[UiUDFParameter])
    restored.attribute shouldBe new Attribute("col", AttributeType.STRING)
    restored.value shouldBe "x"
  }

  it should "default value to the empty string when the JSON omits it" in {
    val json = """{"attribute":{"attributeName":"col","attributeType":"string"}}"""
    val restored = objectMapper.readValue(json, classOf[UiUDFParameter])
    restored.value shouldBe ""
    restored.attribute shouldBe new Attribute("col", AttributeType.STRING)
  }
}
