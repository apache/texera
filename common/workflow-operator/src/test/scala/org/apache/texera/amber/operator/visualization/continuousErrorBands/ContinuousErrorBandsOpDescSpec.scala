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

package org.apache.texera.amber.operator.visualization.continuousErrorBands

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64

class ContinuousErrorBandsOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: ContinuousErrorBandsOpDesc = _

  before {
    opDesc = new ContinuousErrorBandsOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // A column name is carried either literally (plain chunks) or as the
  // base64 payload of a runtime decode site (encoded chunks).
  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def band(x: String, y: String, upper: String, lower: String): BandConfig = {
    val c = new BandConfig()
    c.xValue = x
    c.yValue = y
    c.yUpper = upper
    c.yLower = lower
    c
  }

  it should "default bands to a non-null, empty list" in {
    opDesc.bands should not be null
    opDesc.bands.isEmpty shouldBe true
  }

  it should "throw an AssertionError (not a NullPointerException) when bands is left empty (createPlotlyFigure)" in {
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex shouldBe a[AssertionError]
    ex.getMessage should not be null
    ex.getMessage should include("Bands cannot be empty")
  }

  it should "throw an AssertionError naming the Bands when generating python code with an empty bands list" in {
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should not be null
    ex.getMessage should include("Bands cannot be empty")
  }

  it should "render the configured band columns when the bands list is set" in {
    val bands = new util.ArrayList[BandConfig]()
    bands.add(band("x_col", "y_col", "upper_col", "lower_col"))
    opDesc.bands = bands

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "x_col"))
    assert(carries(figurePlain, "y_col"))
    assert(carries(figurePlain, "upper_col"))
    assert(carries(figurePlain, "lower_col"))

    val code = opDesc.generatePythonCode()
    assert(carries(code, "x_col"))
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }
}
