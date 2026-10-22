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
    val decodeOccurrences = "decode_python_template".r.findAllIn(code).length
    decodeOccurrences should be >= 2
  }

  it should "name the y column and its axis after the selected p-value column" in {
    // The y axis used to read "-log10(pvalue)" whatever column was chosen; a
    // figure drawn from an adjusted column must say so.
    val op = configured
    op.pvalueColumn = "padj"
    val code = op.generatePythonCode()
    code should include(s"table[${decoded("-log10(padj)")}] = -np.log10(")
    code should include(s"y=${decoded("-log10(padj)")}")
    code should include(s"yaxis_title=${decoded("-log10(padj)")}")
    code should not include "-log10(pvalue)"
    code should not include b64("-log10(pvalue)")
  }

  it should "fall back to the column-derived labels when no label is set" in {
    val code = configured.generatePythonCode()
    code should include(s"title=${decoded("Volcano Plot")}")
    code should include(s"xaxis_title=${decoded("log2fc")}")
    code should include(s"yaxis_title=${decoded("-log10(pvalue)")}")
    code should include(s"coloraxis_colorbar_title_text=${decoded("log2fc")}")
  }

  it should "route every configured label through the decoder instead of the default" in {
    // Sentinels with quotes, a backslash and non-ASCII text: as decode sites
    // they cannot break the Python source, and the raw text must never
    // appear in it.
    val op = configured
    op.title = "TITLE_SENT \"quoted\" 火山"
    op.xLabel = "X_LBL_SENT log₂ fold change"
    op.yLabel = "Y_LBL_SENT -log₁₀(BH-adjusted p)"
    op.colorLabel = "COLOR_LBL_SENT \\ back\\slash"
    val code = op.generatePythonCode()
    code should include(s"title=${decoded(op.title)}")
    code should include(s"xaxis_title=${decoded(op.xLabel)}")
    code should include(s"yaxis_title=${decoded(op.yLabel)}")
    code should include(s"coloraxis_colorbar_title_text=${decoded(op.colorLabel)}")
    Seq(op.title, op.xLabel, op.yLabel, op.colorLabel).foreach { sentinel =>
      code should not include sentinel
    }
    // A set label replaces the default rather than being emitted next to it.
    code should not include b64("Volcano Plot")
    code should not include s"xaxis_title=${decoded("log2fc")}"
    code should not include s"yaxis_title=${decoded("-log10(pvalue)")}"
    code should not include s"coloraxis_colorbar_title_text=${decoded("log2fc")}"
    // The y column itself keeps the derived name: a label is text on the axis,
    // not a column rename.
    code should include(s"y=${decoded("-log10(pvalue)")}")
  }

  it should "treat a whitespace-only label as empty and keep the default" in {
    val op = configured
    op.title = " "
    op.xLabel = "\t"
    op.yLabel = "   "
    op.colorLabel = " \n "
    val code = op.generatePythonCode()
    code should include(s"title=${decoded("Volcano Plot")}")
    code should include(s"xaxis_title=${decoded("log2fc")}")
    code should include(s"yaxis_title=${decoded("-log10(pvalue)")}")
    code should include(s"coloraxis_colorbar_title_text=${decoded("log2fc")}")
    Seq(op.title, op.xLabel, op.yLabel, op.colorLabel).foreach { blank =>
      code should not include decoded(blank)
    }
  }

  it should "treat each label independently so one set label leaves the others at their default" in {
    val op = configured
    op.yLabel = "Y_ONLY_SENT"
    val code = op.generatePythonCode()
    code should include(s"yaxis_title=${decoded("Y_ONLY_SENT")}")
    code should include(s"title=${decoded("Volcano Plot")}")
    code should include(s"xaxis_title=${decoded("log2fc")}")
    code should include(s"coloraxis_colorbar_title_text=${decoded("log2fc")}")
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
    code should include(s"title=${decoded("Volcano Plot")}")
    code should include(s"xaxis_title=${decoded("log2fc")}")
    code should include(s"yaxis_title=${decoded("-log10(pvalue)")}")
    code should include(s"coloraxis_colorbar_title_text=${decoded("log2fc")}")
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
