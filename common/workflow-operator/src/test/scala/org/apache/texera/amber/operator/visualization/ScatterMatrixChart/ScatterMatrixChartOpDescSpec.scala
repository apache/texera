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

package org.apache.texera.amber.operator.visualization.ScatterMatrixChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

class ScatterMatrixChartOpDescSpec
    extends AnyFlatSpec
    with BeforeAndAfter
    with GeneratedCodeAssertionChecks {

  var opDesc: ScatterMatrixChartOpDesc = _

  before {
    opDesc = new ScatterMatrixChartOpDesc()
  }

  it should "throw AssertionError with a 'cannot be empty' message when selected attributes are left at their default" in {
    // selectedAttributes now defaults to List() and the assert is null-safe,
    // so an unconfigured operator hits the assert (never an NPE).
    val ex = expectError[AssertionError]("cannot be empty")(opDesc.createPlotlyFigure())
  }

  it should "throw AssertionError with a 'cannot be empty' message when selected attributes list is explicitly empty" in {
    opDesc.selectedAttributes = List()
    val ex = expectError[AssertionError]("cannot be empty")(opDesc.createPlotlyFigure())
  }

  it should "render selected attributes and the color column when configured" in {
    opDesc.selectedAttributes = List("dim_one", "dim_two")
    opDesc.color = "color_col"
    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "dim_one"))
    assert(carries(figurePlain, "dim_two"))
    assert(carries(figurePlain, "color_col"))
    figurePlain should include("px.scatter_matrix")
  }

  it should "generate python code carrying the configured attributes" in {
    opDesc.selectedAttributes = List("dim_one", "dim_two")
    opDesc.color = "color_col"
    val code = opDesc.generatePythonCode()
    assert(carries(code, "dim_one"))
    assert(carries(code, "dim_two"))
    assert(carries(code, "color_col"))
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }
}
