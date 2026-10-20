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

package org.apache.texera.amber.operator.visualization.volcanoPlot

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class VolcanoPlotOpDescSpec extends AnyFlatSpec with Matchers {

  private def configured: VolcanoPlotOpDesc = {
    val op = new VolcanoPlotOpDesc
    op.effectColumn = "log2fc"
    op.pvalueColumn = "pvalue"
    op
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // Every UI-provided string reaches the generated code as this runtime
  // decode expression, never as a literal.
  private def decoded(s: String): String =
    s"self.decode_python_template('${b64(s)}')"

  "VolcanoPlotOpDesc.operatorInfo" should "advertise the user-friendly name and Scientific group" in {
    val info = (new VolcanoPlotOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Volcano Plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.operatorDescription should include("statistical")
  }

  it should "expose exactly one output port wired through forVisualization" in {
    (new VolcanoPlotOpDesc).operatorInfo.outputPorts should have length 1
  }

  "VolcanoPlotOpDesc.getOutputSchemas" should "return a single-port schema with an html-content STRING column" in {
    val op = configured
    val schemas = op.getOutputSchemas(Map.empty)
    schemas should have size 1
    val (portId, schema) = schemas.head
    portId shouldBe op.operatorInfo.outputPorts.head.id
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "html-content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  "VolcanoPlotOpDesc.generatePythonCode" should "render a UDFTableOperator source that decodes both column references" in {
    // EncodableString fields are NOT emitted as literal column names — the
    // pyb macro wraps them in `self.decode_python_template.decode("<base64>")`
    // calls so the column name is resolved at runtime. Verify the structure
    // (class + import + decode site count) instead of substring matches.
    val code = configured.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.express")
    code should include("-log10(pvalue)")
    val decodeOccurrences = "decode_python_template".r.findAllIn(code).length
    decodeOccurrences should be >= 2
  }

  private def decodeSites(code: String): Int =
    "decode_python_template".r.findAllIn(code).length

  it should "keep the first column as the hover label when no hover column is set" in {
    val code = configured.generatePythonCode()
    code should include("hover_name=table.columns[0],")
    // An empty field adds no decode site of its own, and nothing to check for.
    code should not include "hover_name=self.decode_python_template("
    code should not include "Missing hover column"
  }

  it should "answer a set hover column that the table lacks with the operator's error page" in {
    // Like the two required columns, and unlike plotly's uncaught ValueError.
    val op = configured
    op.hoverName = "gene"
    val code = op.generatePythonCode()
    code should include(s"if ${decoded("gene")} not in table.columns:")
    code should include("""self.render_error("Missing hover column in table.")""")
  }

  it should "treat a whitespace-only hover column as empty" in {
    Seq(" ", "\t", " \n ").foreach { blank =>
      val op = configured
      op.hoverName = blank
      val code = op.generatePythonCode()
      code should include("hover_name=table.columns[0],")
      code should not include "Missing hover column"
      code should not include decoded(blank)
    }
  }

  it should "use the configured hover column instead of the first column" in {
    val op = configured
    op.hoverName = "gene"
    val code = op.generatePythonCode()
    code should include(s"hover_name=${decoded("gene")},")
    code should not include "hover_name=table.columns[0]"
    // Exactly two decode sites more than the default: the presence check and the
    // hover_name argument, both naming the hover column.
    decodeSites(code) shouldBe decodeSites(configured.generatePythonCode()) + 2
  }

  it should "route a hover column with quotes and non-ASCII text through the decoder" in {
    val op = configured
    op.hoverName = "HOVER_SENT \"gene\" 基因"
    val code = op.generatePythonCode()
    code should include(s"hover_name=${decoded(op.hoverName)},")
    code should not include "HOVER_SENT"
  }

  it should "render a workflow saved before this change exactly as a default-constructed operator" in {
    val saved = objectMapper
      .readValue(
        """{"operatorType":"VolcanoPlot","effectColumn":"log2fc","pvalueColumn":"pvalue"}""",
        classOf[LogicalOp]
      )
      .asInstanceOf[VolcanoPlotOpDesc]
    val code = saved.generatePythonCode()
    code shouldBe configured.generatePythonCode()
    code should include("hover_name=table.columns[0],")
    code should not include "Missing hover column"
  }

  it should "currently render code even when required fields are empty (no assert guard)" in {
    // Documents the present behavior: VolcanoPlotOpDesc does not assert on
    // its required fields inside `generatePythonCode`. An empty
    // configuration therefore renders syntactically valid Python that
    // references an empty string. The intended contract is split out into
    // the pendingUntilFixed test below so this assertion no longer reads
    // as the contract.
    val op = new VolcanoPlotOpDesc
    val code = op.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }

  it should "eventually reject empty required fields like FunnelPlot/ImageVisualizer (pendingUntilFixed)" in pendingUntilFixed {
    // Intended contract: `effectColumn` and `pvalueColumn` are marked
    // required on `VolcanoPlotOpDesc`, so generatePythonCode on a
    // default-constructed instance should raise instead of producing a
    // string-literal-empty payload. Using pendingUntilFixed so a future
    // validation fix flips this test from Pending to a deliberate failure
    // and forces removal of the marker.
    val op = new VolcanoPlotOpDesc
    intercept[RuntimeException] {
      op.generatePythonCode()
    }
  }
}
