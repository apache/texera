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

package org.apache.texera.amber.operator.visualization.ternaryPlot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

class TernaryPlotOpDescSpec
    extends AnyFlatSpec
    with BeforeAndAfter
    with GeneratedCodeAssertionChecks {

  var opDesc: TernaryPlotOpDesc = _

  before {
    opDesc = new TernaryPlotOpDesc()
  }

  it should "throw AssertionError with a 'cannot be empty' message when all variables are empty" in {
    expectError[AssertionError]("cannot be empty")(opDesc.manipulateTable())
  }

  // The compound assert over the three variables is being split per-field;
  // each message should name the missing variable.
  it should "throw AssertionError naming Variable 1 when only it is missing" in {
    opDesc.secondVariable = "var_b"
    opDesc.thirdVariable = "var_c"
    expectEmptyFieldError("1")(opDesc.manipulateTable())
  }

  it should "throw AssertionError naming Variable 2 when only it is missing" in {
    opDesc.firstVariable = "var_a"
    opDesc.thirdVariable = "var_c"
    expectEmptyFieldError("2")(opDesc.manipulateTable())
  }

  it should "throw AssertionError naming Variable 3 when only it is missing" in {
    opDesc.firstVariable = "var_a"
    opDesc.secondVariable = "var_b"
    expectEmptyFieldError("3")(opDesc.manipulateTable())
  }

  it should "render all three configured variables when set" in {
    opDesc.firstVariable = "var_a"
    opDesc.secondVariable = "var_b"
    opDesc.thirdVariable = "var_c"
    val tablePlain = opDesc.manipulateTable().plain
    assert(carries(tablePlain, "var_a"))
    assert(carries(tablePlain, "var_b"))
    assert(carries(tablePlain, "var_c"))

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "var_a"))
    assert(carries(figurePlain, "var_b"))
    assert(carries(figurePlain, "var_c"))
    figurePlain should include("px.scatter_ternary")
  }
}
