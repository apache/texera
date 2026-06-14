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

package org.apache.texera.amber.operator.visualization.dendrogram

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class DendrogramOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: DendrogramOpDesc = _

  before {
    opDesc = new DendrogramOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  // createDendrogram() is private; generatePythonCode() is the public
  // entry point that reaches its asserts.
  it should "throw AssertionError naming the X column when all fields are empty" in {
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("x")
  }

  it should "throw AssertionError naming the Y column when only xVal and labels are set" in {
    opDesc.xVal = "coord_a"
    opDesc.labels = "label_col"
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("y")
  }

  it should "throw AssertionError naming the Labels column when only xVal and yVal are set" in {
    opDesc.xVal = "coord_a"
    opDesc.yVal = "coord_b"
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("label")
  }

  it should "generate python code carrying all three configured columns" in {
    opDesc.xVal = "coord_a"
    opDesc.yVal = "coord_b"
    opDesc.labels = "label_col"
    val code = opDesc.generatePythonCode()
    assert(carries(code, "coord_a"))
    assert(carries(code, "coord_b"))
    assert(carries(code, "label_col"))
    code should include("create_dendrogram")
    // empty threshold falls back to color_threshold=None
    code should include("color_threshold=None")
  }

  it should "generate python code carrying a non-empty threshold when configured" in {
    opDesc.xVal = "coord_a"
    opDesc.yVal = "coord_b"
    opDesc.labels = "label_col"
    opDesc.threshold = "42.5"
    val code = opDesc.generatePythonCode()
    assert(carries(code, "42.5"))
    code should not include "color_threshold=None"
  }
}
