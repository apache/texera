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

package org.apache.texera.amber.operator.visualization.carpetPlot

import org.apache.texera.amber.operator.visualization.ChartPythonTestSupport.verifyStandaloneChartCases
import org.apache.texera.amber.util.PythonTestSupport.NeedsPythonPackages
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class CarpetPlotOpDescSpec extends AnyFlatSpec with GeneratedCodeAssertionChecks {

  "CarpetPlotOpDesc.operatorInfo" should
    "advertise the name and Scientific visualization group" in {
    val info = (new CarpetPlotOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Carpet Plot"
    info.operatorDescription shouldBe "Visualize data in a Carpet Plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "CarpetPlotOpDesc" should "default a / b / y to the empty string" in {
    val d = new CarpetPlotOpDesc
    d.a shouldBe ""
    d.b shouldBe ""
    d.y shouldBe ""
  }

  "CarpetPlotOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new CarpetPlotOpDesc
    op.getOutputSchemas(Map.empty) shouldBe expectedHtmlOutputSchema(op)
  }

  "CarpetPlotOpDesc.generatePythonCode" should "emit a Plotly Carpet figure" in {
    val d = new CarpetPlotOpDesc
    d.a = "ax"
    d.b = "bx"
    d.y = "yx"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("go.Carpet(")
  }

  // The operator answers bad input with a page saying what is wrong. The script
  // has to answer the same, rather than raising: a column that is not there used
  // to reach the drop as a KeyError, and a value that is not a number reached
  // astype as a ValueError, both of them ending the whole exported run.
  "CarpetPlotOpDesc.generateStandaloneCode" should
    "write the page the operator yields for input it cannot plot" taggedAs NeedsPythonPackages in {
    val d = new CarpetPlotOpDesc
    d.a = "ax"
    d.b = "bx"
    d.y = "yx"
    verifyStandaloneChartCases(
      "carpet",
      d.generateStandaloneCode(),
      casesScript = """cases = {
          |    "good": pd.DataFrame({"ax": [1.0, 2.0], "bx": [1.0, 2.0], "yx": [3.0, 4.0]}),
          |    "missing": pd.DataFrame({"ax": [1.0], "yx": [3.0]}),
          |    "words": pd.DataFrame({"ax": ["one", "two"], "bx": [1.0, 2.0], "yx": [3.0, 4.0]}),
          |    "empty": pd.DataFrame({"ax": [], "bx": [], "yx": []}),
          |    "nulls": pd.DataFrame({"ax": [None, None], "bx": [1.0, 2.0], "yx": [3.0, 4.0]}),
          |}""".stripMargin,
      verdictBody = """    # An answer page is the whole file and opens with the heading, where a
          |    # chart is a plotly document that happens to contain any wording.
          |    if not page.startswith("<h3>"):
          |        verdict = "CHART"
          |    elif page.startswith("<h3>Column '"):
          |        verdict = "COLUMN_NOT_FOUND"
          |    elif page.startswith("<h3>Error converting input columns"):
          |        verdict = "NOT_NUMERIC"
          |    elif page.startswith("<h3>Input table is empty"):
          |        verdict = "EMPTY"
          |    elif page.startswith("<h3>No valid rows"):
          |        verdict = "NO_VALID_ROWS"
          |    else:
          |        verdict = "OTHER_PAGE"""".stripMargin,
      expectedVerdicts = Map(
        "missing" -> "COLUMN_NOT_FOUND",
        "words" -> "NOT_NUMERIC",
        "empty" -> "EMPTY",
        "nulls" -> "NO_VALID_ROWS",
        "good" -> "CHART"
      ),
      plotlyScopeBinding = Some(("plotly.graph_objects", "go")),
      timeoutMessage = "Carpet driver timed out after 120s"
    )
  }

  // The generated code the operator emits for the native path, which the pages
  // above are written to match.
  "CarpetPlotOpDesc.generatePythonCode" should "answer bad input with a page of its own" in {
    val d = new CarpetPlotOpDesc
    d.a = "ax"
    d.b = "bx"
    d.y = "yx"
    val code = d.generatePythonCode()
    code should include("Column '{col}' not found")
    code should include("Error converting input columns to numeric values")
  }

  "CarpetPlotOpDesc" should "round-trip a / b / y through the polymorphic base" in {
    val d = new CarpetPlotOpDesc
    d.a = "ax"
    d.b = "bx"
    d.y = "yx"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[CarpetPlotOpDesc]
    val c = restored.asInstanceOf[CarpetPlotOpDesc]
    c.a shouldBe "ax"
    c.b shouldBe "bx"
    c.y shouldBe "yx"
  }
}
