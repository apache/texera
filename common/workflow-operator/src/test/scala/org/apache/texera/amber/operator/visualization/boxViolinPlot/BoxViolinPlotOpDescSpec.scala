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

package org.apache.texera.amber.operator.visualization.boxViolinPlot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class BoxViolinPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: BoxViolinPlotOpDesc = _

  before {
    opDesc = new BoxViolinPlotOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "throw AssertionError naming the Value Column when value is left empty" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("value")
  }

  it should "render the configured value column in manipulateTable" in {
    opDesc.value = "box_value_col"
    val plain = opDesc.manipulateTable().plain
    assert(carries(plain, "box_value_col"))
    plain should include("dropna")
  }

  it should "generate python code carrying the value column when fully configured" in {
    opDesc.value = "box_value_col"
    opDesc.quartileType = BoxViolinPlotQuartileFunction.LINEAR
    opDesc.horizontalOrientation = false
    opDesc.violinPlot = false
    val code = opDesc.generatePythonCode()
    assert(carries(code, "box_value_col"))
    code should include("quartilemethod")
    code should include("linear")
  }
}
