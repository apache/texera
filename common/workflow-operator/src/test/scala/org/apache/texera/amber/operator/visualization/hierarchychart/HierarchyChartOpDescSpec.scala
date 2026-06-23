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

package org.apache.texera.amber.operator.visualization.hierarchychart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class HierarchyChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: HierarchyChartOpDesc = _

  before {
    opDesc = new HierarchyChartOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // A column name is carried either literally (plain chunks) or as the
  // base64 payload of a runtime decode site (encoded chunks).
  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  // The part of an assert message that names the offending field.
  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "generate a list of hierarchy sections in the python code" in {
    val attributes = Array.fill(3)(new HierarchySection())
    attributes(0).attributeName = "column_a"
    attributes(1).attributeName = "column_b"
    attributes(2).attributeName = "column_c"
    opDesc.hierarchy = attributes.toList
    opDesc.hierarchyChartType = HierarchyChartType.TREEMAP
    opDesc.hierarchyChartType = HierarchyChartType.SUNBURSTCHART
  }

  it should "throw assertion error if hierarchy is empty" in {
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "throw AssertionError naming the Value Column when value is left empty" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("value")
  }

  it should "throw AssertionError naming the hierarchy when the hierarchy path is empty" in {
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("hierarchy")
  }

  it should "render hierarchy attributes and the value column when fully configured" in {
    val sectionA = new HierarchySection()
    sectionA.attributeName = "level_one"
    val sectionB = new HierarchySection()
    sectionB.attributeName = "level_two"
    opDesc.hierarchy = List(sectionA, sectionB)
    opDesc.value = "tree_value_col"
    opDesc.hierarchyChartType = HierarchyChartType.TREEMAP

    val tablePlain = opDesc.manipulateTable().plain
    assert(carries(tablePlain, "tree_value_col"))
    assert(carries(tablePlain, "level_one"))
    assert(carries(tablePlain, "level_two"))

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "tree_value_col"))
    assert(carries(figurePlain, "level_one"))
    assert(carries(figurePlain, "level_two"))
  }
}
