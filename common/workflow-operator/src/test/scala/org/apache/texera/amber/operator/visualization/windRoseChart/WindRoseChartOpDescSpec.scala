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

package org.apache.texera.amber.operator.visualization.windRoseChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class WindRoseChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: WindRoseChartOpDesc = _

  before {
    opDesc = new WindRoseChartOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // A column name is carried either literally (plain chunks) or as the
  // base64 payload of a runtime decode site (encoded chunks).
  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  it should "default rColumn and thetaColumn to non-null, empty strings" in {
    opDesc.rColumn should not be null
    opDesc.rColumn shouldBe ""
    opDesc.thetaColumn should not be null
    opDesc.thetaColumn shouldBe ""
  }

  it should "throw an AssertionError (not a NullPointerException) naming the Radial column when rColumn is empty" in {
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex shouldBe a[AssertionError]
    ex.getMessage should not be null
    ex.getMessage should include("Radial Values (r) column must be selected.")
  }

  it should "throw an AssertionError naming the Angular column when only rColumn is set" in {
    opDesc.rColumn = "r_col"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("Angular Values (θ) column must be selected.")
  }

  it should "render the configured r and theta columns when both are set" in {
    opDesc.rColumn = "r_col"
    opDesc.thetaColumn = "theta_col"

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "r_col"))
    assert(carries(figurePlain, "theta_col"))
    figurePlain should include("px.bar_polar")

    val code = opDesc.generatePythonCode()
    assert(carries(code, "r_col"))
    assert(carries(code, "theta_col"))
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }
}
