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

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

import java.util

class NestedTableOpDescSpec
    extends AnyFlatSpec
    with BeforeAndAfter
    with GeneratedCodeAssertionChecks {

  var opDesc: NestedTableOpDesc = _

  before {
    opDesc = new NestedTableOpDesc()
  }

  private def config(group: String, original: String): NestedTableConfig = {
    val c = new NestedTableConfig()
    c.attributeGroup = group
    c.originalName = original
    c
  }

  it should "default includedColumns to a non-null, empty list" in {
    opDesc.includedColumns should not be null
    opDesc.includedColumns.isEmpty shouldBe true
  }

  it should "throw an AssertionError (not a NullPointerException) when includedColumns is left empty" in {
    val ex =
      expectError[AssertionError]("Included Columns cannot be empty")(opDesc.generatePythonCode())
    ex shouldBe a[AssertionError]
  }

  it should "generate python code carrying the configured columns when includedColumns is set" in {
    val columns = new util.ArrayList[NestedTableConfig]()
    columns.add(config("group_a", "col_one"))
    opDesc.includedColumns = columns

    val code = opDesc.generatePythonCode()
    assert(carries(code, "group_a"))
    assert(carries(code, "col_one"))
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }
}
