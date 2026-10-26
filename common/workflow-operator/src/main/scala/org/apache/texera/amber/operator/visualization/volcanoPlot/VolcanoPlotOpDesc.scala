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

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.{EncodableString, PythonLiteral}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.visualization.PlotlyStandaloneCode
import org.apache.texera.amber.operator.metadata.annotations.{AutofillAttributeName, SampleColumn}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

import javax.validation.constraints.NotNull

// type constraint: the p-value is filtered with `> 0` and passed to np.log10, and the
// effect column is the quantitative x axis, so both can only be numeric.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "effectColumn": { "enum": ["integer", "long", "double"] },
    "pvalueColumn": { "enum": ["integer", "long", "double"] }
  }
}
""")
class VolcanoPlotOpDesc extends PythonOperatorDescriptor with PlotlyStandaloneCode {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Effect Size (log2 Fold Change)")
  @JsonPropertyDescription(
    "Select the column representing the effect size or magnitude " +
      "of change between two experimental groups. This value is typically a log2 fold change " +
      "and is used for the x-axis of the volcano plot."
  )
  @AutofillAttributeName
  @SampleColumn("log2fc")
  @NotNull(message = "Effect Size (log2 Fold Change) cannot be empty")
  var effectColumn: EncodableString = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("P-Value Column")
  @JsonPropertyDescription(
    "Select the column representing the p-value associated with the " +
      "statistical test for each feature. This value is transformed using -log10(p-value) and " +
      "plotted on the y-axis to indicate statistical significance."
  )
  @AutofillAttributeName
  @SampleColumn("pvalue")
  @NotNull(message = "P-Value Column cannot be empty")
  var pvalueColumn: EncodableString = ""

  // Numeric: both are compared against the data and the first is log10'd. contentAs
  // names the boxed class — Option erases its element type, and a blank must not read as 0.
  @JsonProperty(required = false)
  @JsonSchemaTitle("Significance threshold")
  @JsonPropertyDescription(
    "P-value cut-off such as 0.05; draws a dashed horizontal line at -log10 of it"
  )
  @JsonSchemaInject(json = """{"exclusiveMinimum": 0, "maximum": 1}""")
  @JsonDeserialize(contentAs = classOf[java.lang.Double])
  var significanceThreshold: Option[Double] = None

  @JsonProperty(required = false)
  @JsonSchemaTitle("Effect-size threshold")
  @JsonPropertyDescription(
    "Effect-size cut-off such as 1; draws dashed vertical lines at plus and minus it"
  )
  @JsonSchemaInject(json = """{"exclusiveMinimum": 0}""")
  @JsonDeserialize(contentAs = classOf[java.lang.Double])
  var effectSizeThreshold: Option[Double] = None

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("Color by significance")
  @JsonPropertyDescription(
    "Color points Up, Down or Not significant by both thresholds instead of by effect size; " +
      "needs both thresholds, otherwise the effect-size coloring is kept"
  )
  var colorBySignificance: Boolean = false

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      userFriendlyName = "Volcano Plot",
      operatorDescription = "Displays statistical significance versus effect size",
      operatorGroupName = OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  /** A cut-off is used only when it can be drawn: finite, above zero and, for the
    * p-value, at most one. Anything else counts as unset. The schema bounds keep such
    * values out of the property panel; this keeps a hand-edited workflow from failing.
    */
  private def isUsable(cutoff: Double): Boolean =
    !cutoff.isNaN && !cutoff.isInfinite && cutoff > 0

  private def significanceCutoff: Option[Double] =
    significanceThreshold.filter(sig => isUsable(sig) && sig <= 1)

  private def effectCutoff: Option[Double] = effectSizeThreshold.filter(isUsable)

  /** Both cut-offs are needed to call a point Up or Down. */
  private def colorByCategory: Boolean =
    colorBySignificance && significanceCutoff.isDefined && effectCutoff.isDefined

  /** Strict comparisons: a point exactly on a cut-off is not significant. */
  private def significanceColumn: PythonTemplateBuilder =
    if (colorByCategory) {
      val sig = significanceCutoff.get
      val eff = effectCutoff.get
      pyb"""significant = table[$pvalueColumn] < $sig
         |        table["Significance"] = np.select(
         |            [significant & (table[$effectColumn] > $eff), significant & (table[$effectColumn] < -$eff)],
         |            ["Up", "Down"],
         |            default="Not significant",
         |        )
         |"""
    } else pyb""

  // ColorBrewer RdBu end points plus a neutral grey, so the figure reads like other
  // volcano-plot tooling; the continuous RdBu scale stays when not coloring by category.
  // Shared by both code paths so the exported figure cannot drift from the engine's.
  private def categoryColorArgs: PythonLiteral =
    """color="Significance",
      |            color_discrete_map={"Up": "#B2182B", "Down": "#2166AC", "Not significant": "#9E9E9E"},
      |            category_orders={"Significance": ["Up", "Down", "Not significant"]},""".stripMargin

  private def colorArgs: PythonTemplateBuilder =
    if (colorByCategory) pyb"$categoryColorArgs"
    else
      pyb"""color=$effectColumn,
         |            color_continuous_scale="RdBu","""

  // The exported script's twins of the two helpers above: the same Python with the
  // column names as literals rather than decoded, and the very same threshold literals.
  private def standaloneSignificanceColumn: String =
    if (colorByCategory) {
      val sig = significanceCutoff.get
      val eff = effectCutoff.get
      val pvalueLit = pyStringLiteral(pvalueColumn)
      val effectLit = pyStringLiteral(effectColumn)
      s"""significant = table[$pvalueLit] < $sig
         |        table["Significance"] = np.select(
         |            [significant & (table[$effectLit] > $eff), significant & (table[$effectLit] < -$eff)],
         |            ["Up", "Down"],
         |            default="Not significant",
         |        )
         |""".stripMargin
    } else ""

  private def standaloneColorArgs: String =
    if (colorByCategory) categoryColorArgs
    else
      s"""color=${pyStringLiteral(effectColumn)},
         |            color_continuous_scale="RdBu",""".stripMargin

  /** One dashed line per cut-off that is set: horizontal at -log10 of the p-value
    * cut-off, vertical at plus and minus the effect-size cut-off.
    */
  private def thresholdLines: PythonLiteral = {
    val lines = scala.collection.mutable.ArrayBuffer[PythonTemplateBuilder]()
    significanceCutoff.foreach { sig =>
      lines += pyb"""fig.add_hline(y=-np.log10($sig), line_dash="dash", line_color="gray")"""
    }
    effectCutoff.foreach { eff =>
      lines += pyb"""fig.add_vline(x=$eff, line_dash="dash", line_color="gray")"""
      lines += pyb"""fig.add_vline(x=-$eff, line_dash="dash", line_color="gray")"""
    }
    lines.mkString("\n        ")
  }

  override def generatePythonCode(): String = {
    pyb"""
       |from pytexera import *
       |import plotly.express as px
       |import plotly.io
       |import numpy as np
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    def render_error(self, msg):
       |        return f"<h1>Volcano Plot failed</h1><p>{msg}</p>"
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        if table.empty:
       |            yield {"html-content": self.render_error("Input table is empty.")}
       |            return
       |
       |        if $pvalueColumn not in table.columns or $effectColumn not in table.columns:
       |            yield {"html-content": self.render_error("Missing required columns in table.")}
       |            return
       |
       |        # Filter out non-positive p-values to avoid math errors
       |        table = table[table[$pvalueColumn] > 0]
       |        if table.empty:
       |            yield {"html-content": self.render_error("No rows with valid p-values.")}
       |            return
       |
       |        table["-log10(pvalue)"] = -np.log10(table[$pvalueColumn])
       |        $significanceColumn
       |        fig = px.scatter(
       |            table,
       |            x=$effectColumn,
       |            y="-log10(pvalue)",
       |            hover_name=table.columns[0],
       |            $colorArgs
       |            title="Volcano Plot"
       |        )
       |        $thresholdLines
       |
       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
       |        yield {"html-content": html}
       |""".encode
  }

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    val pvalueLit = pyStringLiteral(pvalueColumn)
    val effectLit = pyStringLiteral(effectColumn)
    s"""import numpy as np
       |
       |def render_error(msg):
       |    return f"<h1>Volcano Plot failed</h1><p>{msg}</p>"
       |
       |if in1df.empty:
       |    with open(outputHtml, "w", encoding="utf-8") as output:
       |        output.write(render_error("Input table is empty."))
       |elif $pvalueLit not in in1df.columns or $effectLit not in in1df.columns:
       |    with open(outputHtml, "w", encoding="utf-8") as output:
       |        output.write(render_error("Missing required columns in table."))
       |else:
       |    table = in1df[in1df[$pvalueLit] > 0].copy()
       |    if table.empty:
       |        with open(outputHtml, "w", encoding="utf-8") as output:
       |            output.write(render_error("No rows with valid p-values."))
       |    else:
       |        table["-log10(pvalue)"] = -np.log10(table[$pvalueLit])
       |        $standaloneSignificanceColumn
       |        fig = px.scatter(
       |            table,
       |            x=$effectLit,
       |            y="-log10(pvalue)",
       |            hover_name=table.columns[0],
       |            $standaloneColorArgs
       |            title="Volcano Plot"
       |        )
       |        $thresholdLines
       |        fig.write_json(outputJson)
       |        fig.write_html(outputHtml)
       |        print("Volcano plot saved to " + outputHtml)""".stripMargin
  }

}
