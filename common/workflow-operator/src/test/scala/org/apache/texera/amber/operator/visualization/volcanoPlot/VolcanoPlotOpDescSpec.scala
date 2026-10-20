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
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorMetadataGenerator}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VolcanoPlotOpDescSpec extends AnyFlatSpec with Matchers {

  private def configured: VolcanoPlotOpDesc = {
    val op = new VolcanoPlotOpDesc
    op.effectColumn = "log2fc"
    op.pvalueColumn = "pvalue"
    op
  }

  private val hline = """fig.add_hline(y=-np.log10(0.05), line_dash="dash", line_color="gray")"""
  private val vlines =
    """fig.add_vline(x=1.0, line_dash="dash", line_color="gray")
      |        fig.add_vline(x=-1.0, line_dash="dash", line_color="gray")""".stripMargin
  private val discreteMap =
    """color_discrete_map={"Up": "#B2182B", "Down": "#2166AC", "Not significant": "#9E9E9E"}"""

  private def assertContinuousColoring(code: String): Unit = {
    code should include("""color_continuous_scale="RdBu"""")
    code should not include "Significance"
    code should not include discreteMap
    code should not include "category_orders"
  }

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

  it should "draw no cut-off line and keep the continuous effect-size coloring by default" in {
    val code = configured.generatePythonCode()
    code should not include "add_hline"
    code should not include "add_vline"
    assertContinuousColoring(code)
  }

  it should "draw a dashed horizontal line at -log10 of the significance threshold" in {
    val op = configured
    op.significanceThreshold = Some(0.05)
    val code = op.generatePythonCode()
    code should include(hline)
    code should not include "add_vline"
    assertContinuousColoring(code)
  }

  it should "draw dashed vertical lines at plus and minus the effect-size threshold" in {
    val op = configured
    op.effectSizeThreshold = Some(1.0)
    val code = op.generatePythonCode()
    code should include(vlines)
    code should not include "add_hline"
    assertContinuousColoring(code)
  }

  it should "draw both cut-offs while keeping the continuous coloring when the toggle is off" in {
    val op = configured
    op.significanceThreshold = Some(0.05)
    op.effectSizeThreshold = Some(1.0)
    val code = op.generatePythonCode()
    code should include(hline)
    code should include(vlines)
    assertContinuousColoring(code)
  }

  it should "skip a significance threshold that has no -log10 (zero or negative)" in {
    Seq(0.0, -0.01).foreach { bad =>
      val op = configured
      op.significanceThreshold = Some(bad)
      op.effectSizeThreshold = Some(1.0)
      op.colorBySignificance = true
      val code = op.generatePythonCode()
      code should not include "add_hline"
      code should not include "np.log10(0.0)"
      code should not include "np.log10(-0.01)"
      // The effect-size lines still draw, and without a usable p-value cut-off
      // no point can be called Up or Down, so the coloring stays continuous.
      code should include(vlines)
      assertContinuousColoring(code)
    }
  }

  it should "skip an effect-size threshold that is zero or negative" in {
    // A negative cut-off would flip the Up and Down comparisons and call small
    // effects significant; zero is the same boundary and is ruled out too.
    Seq(0.0, -1.0).foreach { bad =>
      val op = configured
      op.significanceThreshold = Some(0.05)
      op.effectSizeThreshold = Some(bad)
      op.colorBySignificance = true
      val code = op.generatePythonCode()
      code should not include "add_vline"
      code should include(hline)
      assertContinuousColoring(code)
    }
  }

  it should "skip a significance threshold above one, whose line would sit below the axis" in {
    val op = configured
    op.significanceThreshold = Some(2.0)
    op.effectSizeThreshold = Some(1.0)
    op.colorBySignificance = true
    val code = op.generatePythonCode()
    code should not include "add_hline"
    code should include(vlines)
    assertContinuousColoring(code)
  }

  it should "color Up, Down and Not significant when both thresholds are set and the toggle is on" in {
    val op = configured
    op.significanceThreshold = Some(0.05)
    op.effectSizeThreshold = Some(1.0)
    op.colorBySignificance = true
    val code = op.generatePythonCode()
    // Strict comparisons on the selected p-value column and the effect column:
    // a point exactly on a cut-off is not significant.
    code should include("significant = table[self.decode_python_template(")
    code should include("] < 0.05")
    code should include("] > 1.0)")
    code should include("] < -1.0)")
    code should include("""table["Significance"] = np.select(""")
    code should include("""["Up", "Down"],""")
    code should include("""default="Not significant",""")
    code should include("""color="Significance",""")
    code should include(discreteMap)
    code should include("""category_orders={"Significance": ["Up", "Down", "Not significant"]}""")
    code should not include "color_continuous_scale"
    code should include(hline)
    code should include(vlines)
  }

  it should "accept a significance threshold of exactly one, the schema maximum, and draw it at y = 0" in {
    val op = configured
    op.significanceThreshold = Some(1.0)
    op.effectSizeThreshold = Some(1.0)
    op.colorBySignificance = true
    val code = op.generatePythonCode()
    code should include("""fig.add_hline(y=-np.log10(1.0), line_dash="dash", line_color="gray")""")
    code should include(vlines)
    code should include("""color="Significance",""")
    code should include("] < 1.0")
  }

  it should "emit every threshold as a literal Python float, exponent forms included" in {
    val pyFloat = """^-?\d+(\.\d+)?([eE][-+]?\d+)?$""".r
    // The literal after np.log10( or add_vline(x= ; the y column's np.log10(table[...]) does
    // not match because its argument does not start with a digit.
    val emittedLiterals = """np\.log10\(([0-9.Ee+-]+)\)|add_vline\(x=-?([0-9.Ee+-]+),""".r
    Seq(
      (1e-300, 1e21, "1.0E-300", "1.0E21"),
      (java.lang.Double.MIN_VALUE, 1.0, "4.9E-324", "1.0")
    ).foreach {
      case (sig, eff, sigLit, effLit) =>
        val op = configured
        op.significanceThreshold = Some(sig)
        op.effectSizeThreshold = Some(eff)
        op.colorBySignificance = true
        val code = op.generatePythonCode()
        val literals = emittedLiterals
          .findAllMatchIn(code)
          .map(m => Option(m.group(1)).getOrElse(m.group(2)))
          .toSeq
        literals should contain theSameElementsAs Seq(sigLit, effLit, effLit)
        literals.foreach(lit => pyFloat.findFirstIn(lit) shouldBe Some(lit))
        // The category comparisons use the very same literals.
        code should include(s"] < $sigLit")
        code should include(s"] > $effLit)")
        code should include(s"] < -$effLit)")
    }
  }

  it should "fall back to the continuous coloring when the toggle is on but a threshold is missing" in {
    val onlySignificance = configured
    onlySignificance.significanceThreshold = Some(0.05)
    onlySignificance.colorBySignificance = true
    val sigCode = onlySignificance.generatePythonCode()
    sigCode should include(hline)
    sigCode should not include "add_vline"
    assertContinuousColoring(sigCode)

    val onlyEffect = configured
    onlyEffect.effectSizeThreshold = Some(1.0)
    onlyEffect.colorBySignificance = true
    val effCode = onlyEffect.generatePythonCode()
    effCode should include(vlines)
    effCode should not include "add_hline"
    assertContinuousColoring(effCode)

    val neither = configured
    neither.colorBySignificance = true
    assertContinuousColoring(neither.generatePythonCode())
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
    code should not include "add_hline"
    code should not include "add_vline"
    assertContinuousColoring(code)
  }

  /** Reads the shapes a stored workflow can hold. Without `contentAs` a JSON string
    * stays unconverted inside the Option and the first use throws.
    */
  private def readOp(json: String): VolcanoPlotOpDesc =
    objectMapper
      .readValue(s"""{"operatorType":"VolcanoPlot"$json}""", classOf[LogicalOp])
      .asInstanceOf[VolcanoPlotOpDesc]

  "VolcanoPlotOpDesc thresholds" should "deserialize JSON numbers" in {
    val op = readOp(""","significanceThreshold":0.05,"effectSizeThreshold":1""")
    op.significanceThreshold shouldBe Some(0.05)
    op.effectSizeThreshold shouldBe Some(1.0)
  }

  it should "deserialize numeric strings" in {
    val op = readOp(""","significanceThreshold":"0.05","effectSizeThreshold":"1.5"""")
    op.significanceThreshold shouldBe Some(0.05)
    op.effectSizeThreshold shouldBe Some(1.5)
  }

  it should "read an absent, null or blank value as unset rather than as zero" in {
    readOp("").significanceThreshold shouldBe None
    readOp("").effectSizeThreshold shouldBe None
    val nulls = readOp(""","significanceThreshold":null,"effectSizeThreshold":null""")
    nulls.significanceThreshold shouldBe None
    nulls.effectSizeThreshold shouldBe None
    val blanks = readOp(""","significanceThreshold":"","effectSizeThreshold":""""")
    blanks.significanceThreshold shouldBe None
    blanks.effectSizeThreshold shouldBe None
  }

  it should "hold Doubles, not the raw JSON values" in {
    val op = readOp(""","significanceThreshold":"0.05","effectSizeThreshold":"1.5"""")
    op.significanceThreshold.map(_ * 2) shouldBe Some(0.1)
    op.effectSizeThreshold.map(_ * 2) shouldBe Some(3.0)
  }

  it should "treat a non-finite value read from JSON as unset so no Infinity or NaN reaches the Python" in {
    Seq(""""Infinity"""", """"-Infinity"""", """"NaN"""").foreach { raw =>
      val op = readOp(
        s""","significanceThreshold":$raw,"effectSizeThreshold":$raw,"colorBySignificance":true"""
      )
      // Jackson does read the text as a Double; the operator must be the one to drop it.
      op.significanceThreshold.exists(d => d.isNaN || d.isInfinite) shouldBe true
      op.effectSizeThreshold.exists(d => d.isNaN || d.isInfinite) shouldBe true
      op.effectColumn = "log2fc"
      op.pvalueColumn = "pvalue"
      val code = op.generatePythonCode()
      code should not include "add_hline"
      code should not include "add_vline"
      code should not include "Infinity"
      code should not include "NaN"
      assertContinuousColoring(code)
    }
  }

  "VolcanoPlotOpDesc schema" should "bound both thresholds so the property panel rejects what the operator ignores" in {
    val props = OperatorMetadataGenerator
      .generateOperatorJsonSchema(classOf[VolcanoPlotOpDesc])
      .path("properties")
    props.path("significanceThreshold").path("exclusiveMinimum").asDouble() shouldBe 0.0
    props.path("significanceThreshold").path("maximum").asDouble() shouldBe 1.0
    props.path("effectSizeThreshold").path("exclusiveMinimum").asDouble() shouldBe 0.0
    props.path("effectSizeThreshold").has("maximum") shouldBe false
  }

  "VolcanoPlotOpDesc.colorBySignificance" should "default to false and read a saved value" in {
    readOp("").colorBySignificance shouldBe false
    readOp(""","colorBySignificance":true""").colorBySignificance shouldBe true
    readOp(""","colorBySignificance":false""").colorBySignificance shouldBe false
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
