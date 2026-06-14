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

package org.apache.texera.amber.operator.visualization.ternaryContour

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class TernaryContourOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: TernaryContourOpDesc = _

  before {
    opDesc = new TernaryContourOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "throw AssertionError with a 'cannot be empty' message when all variables are empty" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
  }

  // The compound assert over the four variables is split per-field;
  // each message should name the missing variable.
  it should "throw AssertionError naming Variable 1 when only it is missing" in {
    opDesc.secondVariable = "var_b"
    opDesc.thirdVariable = "var_c"
    opDesc.fourthVariable = "var_d"
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("1")
  }

  it should "throw AssertionError naming Variable 2 when only it is missing" in {
    opDesc.firstVariable = "var_a"
    opDesc.thirdVariable = "var_c"
    opDesc.fourthVariable = "var_d"
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("2")
  }

  it should "throw AssertionError naming Variable 3 when only it is missing" in {
    opDesc.firstVariable = "var_a"
    opDesc.secondVariable = "var_b"
    opDesc.fourthVariable = "var_d"
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("3")
  }

  it should "throw AssertionError naming the Measured Value when only it is missing" in {
    opDesc.firstVariable = "var_a"
    opDesc.secondVariable = "var_b"
    opDesc.thirdVariable = "var_c"
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("measured value")
  }

  it should "render all four configured variables when set" in {
    opDesc.firstVariable = "var_a"
    opDesc.secondVariable = "var_b"
    opDesc.thirdVariable = "var_c"
    opDesc.fourthVariable = "var_d"
    val tablePlain = opDesc.manipulateTable().plain
    assert(carries(tablePlain, "var_a"))
    assert(carries(tablePlain, "var_b"))
    assert(carries(tablePlain, "var_c"))
    assert(carries(tablePlain, "var_d"))

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "var_a"))
    assert(carries(figurePlain, "var_b"))
    assert(carries(figurePlain, "var_c"))
    assert(carries(figurePlain, "var_d"))
    figurePlain should include("create_ternary_contour")
  }
}
