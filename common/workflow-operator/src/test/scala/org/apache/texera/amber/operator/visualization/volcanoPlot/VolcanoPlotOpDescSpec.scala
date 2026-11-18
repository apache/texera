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
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

class VolcanoPlotOpDescSpec extends AnyFlatSpec with GeneratedCodeAssertionChecks {

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
    code should not include "np.select"
    code should not include "Significance"
    code should not include discreteMap
    code should not include "category_orders"
  }

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
      op.generatePythonCode() shouldBe configured.generatePythonCode()
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

  it should "draw no cut-off line and keep the continuous effect-size coloring by default" in {
    val code = configured.generatePythonCode()
    code should not include "add_hline"
    code should not include "add_vline"
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
    code should include("significance = np.select(")
    code should include("""["Up", "Down"],""")
    code should include("""default="Not significant",""")
    code should include("color=significance,")
    code should include("""labels={"color": "Significance"},""")
    code should include(discreteMap)
    code should include("""category_orders={"color": ["Up", "Down", "Not significant"]}""")
    code should not include "color_continuous_scale"
    code should include(hline)
    code should include(vlines)
  }

  it should "leave an input column named Significance alone, in the exported script too" in {
    // Writing the categories into table["Significance"] would overwrite an input column
    // of that name before px reads the table; as the effect column, the x axis would
    // show the category labels. The categories go to px directly instead.
    val op = configured
    op.effectColumn = "Significance"
    op.significanceThreshold = Some(0.05)
    op.effectSizeThreshold = Some(1.0)
    op.colorBySignificance = true
    Seq(op.generatePythonCode(), op.generateStandaloneCode()).foreach { code =>
      code should not include """table["Significance"] ="""
    }
  }

  it should "accept a significance threshold of exactly one, the schema maximum, and draw it at y = 0" in {
    val op = configured
    op.significanceThreshold = Some(1.0)
    op.effectSizeThreshold = Some(1.0)
    op.colorBySignificance = true
    val code = op.generatePythonCode()
    code should include("""fig.add_hline(y=-np.log10(1.0), line_dash="dash", line_color="gray")""")
    code should include(vlines)
    code should include("color=significance,")
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
    op.yLabel = "Y_LBL_SENT -log₁₀(BH-adjusted p) \\ back\\slash"
    val code = op.generatePythonCode()
    code should include(s"title=${decoded(op.title)}")
    code should include(s"xaxis_title=${decoded(op.xLabel)}")
    code should include(s"yaxis_title=${decoded(op.yLabel)}")
    // The color bar follows the x label, since both show the effect column.
    code should include(s"coloraxis_colorbar_title_text=${decoded(op.xLabel)}")
    Seq(op.title, op.xLabel, op.yLabel).foreach { sentinel =>
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
    op.yLabel = " \n "
    op.generatePythonCode() shouldBe configured.generatePythonCode()
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
    saved.generatePythonCode() shouldBe configured.generatePythonCode()
  }

  "VolcanoPlotOpDesc.generateStandaloneCode" should "keep the first column as the hover label when no hover column is set" in {
    val code = configured.generateStandaloneCode()
    code should include("hover_name=table.columns[0],")
    code should not include "Missing hover column"
  }

  it should "use the configured hover column as a literal and check that the table has it" in {
    val op = configured
    op.hoverName = "gene"
    val code = op.generateStandaloneCode()
    code should include("""hover_name="gene",""")
    code should not include "hover_name=table.columns[0]"
    // One more branch of the script's column checks, before the row filter and
    // right before the drawing branch, so a missing hover column gets the error
    // page and not plotly's ValueError.
    code should include(
      """        output.write(render_error("Missing required columns in table."))
        |elif "gene" not in in1df.columns:
        |    with open(outputHtml, "w", encoding="utf-8") as output:
        |        output.write(render_error("Missing hover column in table."))
        |else:""".stripMargin
    )
  }

  it should "escape a hover column with quotes or a backslash rather than decode it" in {
    // The export has no decoder, so the name itself is in the script: a quote or a
    // backslash must come out escaped, or the literal would end early.
    val op = configured
    op.hoverName = "HOVER_SENT \"gene\" 基因"
    val code = op.generateStandaloneCode()
    code should include("hover_name=\"HOVER_SENT \\\"gene\\\" 基因\",")
    code should include("elif \"HOVER_SENT \\\"gene\\\" 基因\" not in in1df.columns:")
    code should not include "decode_python_template"
    op.hoverName = "gene\\id"
    op.generateStandaloneCode() should include("hover_name=\"gene\\\\id\",")
  }

  it should "treat a whitespace-only hover column as empty" in {
    Seq(" ", "\t", " \n ").foreach { blank =>
      val op = configured
      op.hoverName = blank
      op.generateStandaloneCode() shouldBe configured.generateStandaloneCode()
    }
  }

  it should "draw no cut-off line and keep the continuous effect-size coloring by default" in {
    val code = configured.generateStandaloneCode()
    code should not include "add_hline"
    code should not include "add_vline"
    assertContinuousColoring(code)
    code should include("""color="log2fc",""")
  }

  it should "color Up, Down and Not significant with the column names as literals" in {
    val op = configured
    op.significanceThreshold = Some(0.05)
    op.effectSizeThreshold = Some(1.0)
    op.colorBySignificance = true
    val code = op.generateStandaloneCode()
    code should include("""significant = table["pvalue"] < 0.05""")
    code should include(
      """[significant & (table["log2fc"] > 1.0), significant & (table["log2fc"] < -1.0)]"""
    )
    code should include("color=significance,")
    code should include("""labels={"color": "Significance"},""")
    code should include(discreteMap)
    code should include("""category_orders={"color": ["Up", "Down", "Not significant"]},""")
    code should not include """color_continuous_scale="RdBu""""
    code should include(hline)
    code should include(vlines)
    code should not include "decode_python_template"
  }

  it should "make the same cut-off and coloring decisions as the engine" in {
    // The export is drawn from the same figure, so for every shape a stored
    // threshold can take the two paths must agree on which lines to draw and on
    // whether to color by category; a rule applied on one side only would make
    // the exported figure differ from the one shown in the workflow.
    val cutOffLine = """fig\.add_[hv]line\([^\n]*""".r
    Seq(
      (None, None, false),
      (Some(0.05), Some(1.0), false),
      (Some(0.05), Some(1.0), true),
      (Some(0.05), None, true),
      (Some(0.0), Some(1.0), true),
      (Some(1.5), Some(-1.0), true),
      (Some(Double.NaN), Some(Double.PositiveInfinity), true),
      (Some(1e-300), Some(1e21), true)
    ).foreach {
      case (sig, eff, toggle) =>
        val op = configured
        op.significanceThreshold = sig
        op.effectSizeThreshold = eff
        op.colorBySignificance = toggle
        val engine = op.generatePythonCode()
        val export = op.generateStandaloneCode()
        cutOffLine.findAllIn(export).toSeq shouldBe cutOffLine.findAllIn(engine).toSeq
        export.contains("Significance") shouldBe engine.contains("Significance")
        export.contains(discreteMap) shouldBe engine.contains(discreteMap)
    }
  }

  it should "name the y column and its axis after the selected p-value column" in {
    val op = configured
    op.pvalueColumn = "padj"
    val code = op.generateStandaloneCode()
    code should include("""table["-log10(padj)"] = -np.log10(table["padj"])""")
    code should include("""y="-log10(padj)",""")
    code should include("""yaxis_title="-log10(padj)",""")
    code should not include "-log10(pvalue)"
    // The derived y column is the one literal built from a column name rather than
    // taken from a field, so a quote in that name must survive the wrap and the escape.
    op.pvalueColumn = "p\"adj"
    val quoted = op.generateStandaloneCode()
    quoted should include("table[\"-log10(p\\\"adj)\"] = -np.log10(table[\"p\\\"adj\"])")
    quoted should include("yaxis_title=\"-log10(p\\\"adj)\",")
  }

  it should "fall back to the column-derived labels when no label is set" in {
    val code = configured.generateStandaloneCode()
    code should include("""title="Volcano Plot"""")
    code should include("""xaxis_title="log2fc",""")
    code should include("""yaxis_title="-log10(pvalue)",""")
    code should include("""coloraxis_colorbar_title_text="log2fc"""")
  }

  it should "emit every configured label as an escaped Python literal" in {
    // The export has no decoder, so the text itself is in the script: a quote or
    // a backslash must come out escaped, or the label would end the literal early.
    val op = configured
    op.title = "TITLE_SENT \"quoted\" 火山"
    op.xLabel = "X_LBL_SENT log₂ fold change"
    op.yLabel = "Y_LBL_SENT -log₁₀(BH-adjusted p) \\ back\\slash"
    val code = op.generateStandaloneCode()
    code should include("title=\"TITLE_SENT \\\"quoted\\\" 火山\"")
    code should include("xaxis_title=\"X_LBL_SENT log₂ fold change\",")
    code should include("yaxis_title=\"Y_LBL_SENT -log₁₀(BH-adjusted p) \\\\ back\\\\slash\",")
    // The color bar follows the x label, since both show the effect column.
    code should include("coloraxis_colorbar_title_text=\"X_LBL_SENT log₂ fold change\"")
    // A set label replaces the default rather than being emitted next to it.
    code should not include """title="Volcano Plot""""
    code should not include """xaxis_title="log2fc""""
    code should not include """yaxis_title="-log10(pvalue)""""
    code should not include """coloraxis_colorbar_title_text="log2fc""""
    // The y column itself keeps the derived name: a label is text on the axis,
    // not a column rename.
    code should include("""y="-log10(pvalue)",""")
  }

  it should "treat a whitespace-only label as empty and keep the default" in {
    val op = configured
    op.title = " "
    op.xLabel = "\t"
    op.yLabel = " \n "
    op.generateStandaloneCode() shouldBe configured.generateStandaloneCode()
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

  "VolcanoPlotOpDesc.generatePythonCode" should "currently render code even when required fields are empty (no assert guard)" in {
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
