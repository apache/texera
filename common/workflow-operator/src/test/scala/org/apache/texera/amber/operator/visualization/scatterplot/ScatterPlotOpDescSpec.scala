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

package org.apache.texera.amber.operator.visualization.scatterplot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ScatterPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: ScatterplotOpDesc = _

  before {
    opDesc = new ScatterplotOpDesc()
  }

  // xColumn/yColumn are `private val`s on the descriptor (normally populated
  // by Jackson); set them reflectively so the assert-bearing methods can be
  // exercised on both their negative and positive paths.
  private def setPrivateField(name: String, value: String): Unit = {
    val field = opDesc.getClass.getDeclaredFields
      .find(_.getName.endsWith(name))
      .getOrElse(fail(s"field $name not found on ScatterplotOpDesc"))
    field.setAccessible(true)
    field.set(opDesc, value)
  }

  // The part of an assert message that names the offending field.
  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "throw assertion error if chart is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "throw AssertionError naming the X-Column when both columns are empty" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("x")
  }

  it should "throw AssertionError naming the Y-Column when only xColumn is set" in {
    setPrivateField("xColumn", "scatter_x")
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("y")
  }

  it should "throw AssertionError with a per-field message in createPlotlyFigure when only yColumn is set" in {
    setPrivateField("yColumn", "scatter_y")
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("x")
  }

  it should "render both configured columns when xColumn and yColumn are set" in {
    setPrivateField("xColumn", "scatter_x")
    setPrivateField("yColumn", "scatter_y")
    val tablePlain = opDesc.manipulateTable().plain
    tablePlain should include("scatter_x")
    tablePlain should include("scatter_y")
    noException should be thrownBy opDesc.createPlotlyFigure()
  }

}
