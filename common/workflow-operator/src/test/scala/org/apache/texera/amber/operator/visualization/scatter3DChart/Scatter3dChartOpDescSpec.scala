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

package org.apache.texera.amber.operator.visualization.scatter3DChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class Scatter3dChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: Scatter3dChartOpDesc = _

  before {
    opDesc = new Scatter3dChartOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  // createPlotlyFigure() is private; generatePythonCode() is the public
  // entry point that reaches its asserts.
  it should "throw AssertionError naming the X Column when all fields are empty" in {
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("x")
  }

  it should "throw AssertionError naming the Y Column when only x and z are set" in {
    opDesc.x = "col_x"
    opDesc.z = "col_z"
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("y")
  }

  it should "throw AssertionError naming the Z Column when only x and y are set" in {
    opDesc.x = "col_x"
    opDesc.y = "col_y"
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("z")
  }

  it should "generate python code carrying all three configured columns" in {
    opDesc.x = "col_x"
    opDesc.y = "col_y"
    opDesc.z = "col_z"
    val code = opDesc.generatePythonCode()
    assert(carries(code, "col_x"))
    assert(carries(code, "col_y"))
    assert(carries(code, "col_z"))
    code should include("go.Scatter3d")
  }
}
