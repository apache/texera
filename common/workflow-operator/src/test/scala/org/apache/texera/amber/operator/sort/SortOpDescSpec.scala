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

package org.apache.texera.amber.operator.sort

import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.funsuite.AnyFunSuite

class SortOpDescSpec extends AnyFunSuite {

  private def deserialize(json: String): SortOpDesc =
    objectMapper.readValue(json, classOf[LogicalOp]).asInstanceOf[SortOpDesc]

  test("deserializes JSON that omits the attributes field into an empty list, not null") {
    val desc = deserialize("""{"operatorType": "Sort"}""")
    assert(desc.attributes != null)
    assert(desc.attributes.isEmpty)
  }

  test("deserializes JSON with an explicit empty attributes array") {
    val desc = deserialize("""{"operatorType": "Sort", "attributes": []}""")
    assert(desc.attributes.isEmpty)
  }

  test("deserializes JSON with a fully configured sort key") {
    val json =
      """{"operatorType": "Sort", "attributes": [{"attribute": "age", "sortPreference": "DESC"}]}"""
    val desc = deserialize(json)
    assert(desc.attributes.size == 1)
    assert(desc.attributes.head.attributeName == "age")
    assert(desc.attributes.head.sortPreference == SortPreference.DESC)
    val code = desc.generatePythonCode()
    assert(code.contains("ascending_orders = [False]")) // DESC maps to ascending=False
  }

  test("generatePythonCode raises a clear message when no sort key is configured") {
    val desc = deserialize("""{"operatorType": "Sort"}""")
    val ex = intercept[IllegalArgumentException](desc.generatePythonCode())
    assert(ex.getMessage.contains("at least one sort key"))
  }

  test("generatePythonCode raises a clear message when a sort key has no attribute") {
    val json =
      """{"operatorType": "Sort", "attributes": [{"attribute": "", "sortPreference": "ASC"}]}"""
    val desc = deserialize(json)
    val ex = intercept[IllegalArgumentException](desc.generatePythonCode())
    assert(ex.getMessage.contains("must have an attribute selected"))
  }
}
