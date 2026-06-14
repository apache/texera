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

package org.apache.texera.amber.operator.visualization.choroplethMap

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class ChoroplethMapOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: ChoroplethMapOpDesc = _

  before {
    opDesc = new ChoroplethMapOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "throw AssertionError naming the Locations Column when both fields are empty" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("location")
  }

  it should "throw AssertionError naming the Color Column when only locations is set" in {
    opDesc.locations = "iso_code"
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("color")
  }

  it should "throw AssertionError naming the Locations Column when only color is set" in {
    opDesc.color = "intensity"
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("location")
  }

  it should "throw AssertionError with a per-field message in createPlotlyFigure when only locations is set" in {
    // The compound assert(locations.nonEmpty && color.nonEmpty) is being split
    // per-field; the failing message should name the field that is missing.
    opDesc.locations = "iso_code"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("color")
  }

  it should "render both configured columns when locations and color are set" in {
    opDesc.locations = "iso_code"
    opDesc.color = "intensity"
    val tablePlain = opDesc.manipulateTable().plain
    assert(carries(tablePlain, "iso_code"))
    assert(carries(tablePlain, "intensity"))

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "iso_code"))
    assert(carries(figurePlain, "intensity"))
    figurePlain should include("px.choropleth")
  }
}
