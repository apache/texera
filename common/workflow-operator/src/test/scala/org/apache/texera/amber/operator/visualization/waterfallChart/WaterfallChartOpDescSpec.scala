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

package org.apache.texera.amber.operator.visualization.waterfallChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class WaterfallChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: WaterfallChartOpDesc = _

  before {
    opDesc = new WaterfallChartOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // A column name is carried either literally (plain chunks) or as the
  // base64 payload of a runtime decode site (encoded chunks).
  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  it should "default xColumn and yColumn to non-null, empty strings" in {
    opDesc.xColumn should not be null
    opDesc.xColumn shouldBe ""
    opDesc.yColumn should not be null
    opDesc.yColumn shouldBe ""
  }

  it should "throw an AssertionError (not a NullPointerException) naming the X Axis when xColumn is left empty" in {
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex shouldBe a[AssertionError]
    ex.getMessage should not be null
    ex.getMessage should include("X Axis Values cannot be empty")
  }

  it should "throw an AssertionError naming the Y Axis when only xColumn is set" in {
    opDesc.xColumn = "x_col"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("Y Axis Values cannot be empty")
  }

  it should "render the configured x and y columns when both are set" in {
    opDesc.xColumn = "x_col"
    opDesc.yColumn = "y_col"

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "x_col"))
    assert(carries(figurePlain, "y_col"))
    figurePlain should include("go.Waterfall")

    val code = opDesc.generatePythonCode()
    assert(carries(code, "x_col"))
    assert(carries(code, "y_col"))
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }
}
