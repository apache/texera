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

package org.apache.texera.amber.operator.visualization.radarChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class RadarChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: RadarChartOpDesc = _

  before {
    opDesc = new RadarChartOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "throw AssertionError naming the Name Column when nothing is configured" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("name")
  }

  it should "throw AssertionError naming the Value Columns when only nameColumn is set (null list)" in {
    opDesc.nameColumn = "entity"
    // valueColumns defaults to null; the assert guards null before nonEmpty
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("value")
  }

  it should "throw AssertionError naming the Value Columns when the list is explicitly empty" in {
    opDesc.nameColumn = "entity"
    opDesc.valueColumns = List()
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("value")
  }

  it should "throw AssertionError naming the Value Columns in createPlotlyFigure when the list is unset" in {
    // createPlotlyFigure now guards valueColumns directly (null-safe), so
    // calling it on an unconfigured operator fails fast with a message
    // instead of an NPE from mapping over null.
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("value")
  }

  it should "throw AssertionError naming the Value Columns in createPlotlyFigure when the list is explicitly empty" in {
    opDesc.nameColumn = "entity"
    opDesc.valueColumns = List()
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("value")
  }

  it should "render the name column and all value columns when configured" in {
    opDesc.nameColumn = "entity"
    opDesc.valueColumns = List("metric_one", "metric_two")
    val tablePlain = opDesc.manipulateTable().plain
    assert(carries(tablePlain, "entity"))
    assert(carries(tablePlain, "metric_one"))
    assert(carries(tablePlain, "metric_two"))
  }

  it should "generate python code carrying the configured columns" in {
    opDesc.nameColumn = "entity"
    opDesc.valueColumns = List("metric_one", "metric_two")
    val code = opDesc.generatePythonCode()
    assert(carries(code, "entity"))
    assert(carries(code, "metric_one"))
    assert(carries(code, "metric_two"))
    code should include("go.Scatterpolar")
  }
}
