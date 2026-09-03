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

package org.apache.texera.amber.operator.visualization.ecdfPlot

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class ECDFPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: ECDFPlotOpDesc = _

  before {
    opDesc = new ECDFPlotOpDesc()
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

  it should "throw assertion error if value column is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "generate a plotly ecdf figure with optional parameters" in {
    opDesc.valueColumn = "score"
    opDesc.colorColumn = "group"
    opDesc.separateBy = "category"
    opDesc.yAxisMode = "count"
    opDesc.cdfMode = "reversed"
    opDesc.orientation = "horizontal"
    opDesc.showMarkers = true
    opDesc.marginal = "histogram"

    val plain = opDesc.createPlotlyFigure().plain

    assert(plain.contains("fig = px.ecdf(table"))
    assert(plain.contains("ecdfnorm=None"))
    assert(plain.contains("orientation='h'"))
    assert(plain.contains("markers=True"))
    // Assert WHICH value reaches each keyword, not merely that a decode site is
    // present there. Every one of these arguments renders to the same marker, so a
    // presence-only check cannot tell x from color, or ecdfmode from marginal, and
    // the payloads stay freely substitutable underneath it.
    assert(plain.contains(s"x=$decodeSite('${b64("score")}')"))
    assert(plain.contains(s"color=$decodeSite('${b64("group")}')"))
    assert(plain.contains(s"facet_col=$decodeSite('${b64("category")}')"))
    assert(plain.contains(s"ecdfmode=$decodeSite('${b64("reversed")}')"))
    assert(plain.contains(s"marginal=$decodeSite('${b64("histogram")}')"))
  }

  it should "throw AssertionError naming the Value Column when valueColumn is left empty (manipulateTable)" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("value")
  }

  it should "throw AssertionError naming the Value Column when valueColumn is left empty (createPlotlyFigure)" in {
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("value")
  }

  it should "carry the configured value column through manipulateTable and the generated code" in {
    opDesc.valueColumn = "ecdf_value_col"
    val tablePlain = opDesc.manipulateTable().plain
    assert(carries(tablePlain, "ecdf_value_col"))

    val code = opDesc.generatePythonCode()
    assert(carries(code, "ecdf_value_col"))
  }

  // --- helpers for the argument-shape assertions -----------------------------

  // Every column name spliced into the emitted Python becomes a decode site, so
  // counting them on one line tells us how many columns that line references.
  private val decodeSite = "self.decode_python_template"

  private def decodeSiteCount(s: String): Int =
    s.split(java.util.regex.Pattern.quote(decodeSite), -1).length - 1

  private def lineContaining(code: String, marker: String): String =
    code.linesIterator
      .find(_.contains(marker))
      .getOrElse(fail(s"no line containing '$marker' in:\n$code"))

  // --- operator metadata ------------------------------------------------------

  "ECDFPlotOpDesc.operatorInfo" should
    "advertise the visualization name, group, and a 1-in/1-out shape" in {
    val info = opDesc.operatorInfo
    info.userFriendlyName shouldBe "Empirical Cumulative Distribution Plot"
    info.operatorDescription shouldBe
      "Visualize the empirical cumulative distribution of a numeric column."
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "ECDFPlotOpDesc.getOutputSchemas" should
    "emit a single html-content STRING column keyed by the declared output port" in {
    opDesc.valueColumn = "score"
    val inputPort = opDesc.operatorInfo.inputPorts.head.id
    val out = opDesc.getOutputSchemas(
      Map(inputPort -> Schema().add("score", AttributeType.DOUBLE))
    )

    out.keySet shouldBe Set(opDesc.operatorInfo.outputPorts.head.id)
    val schema = out(opDesc.operatorInfo.outputPorts.head.id)
    schema.getAttributeNames should contain theSameElementsAs List("html-content")
    schema.getAttribute("html-content").getType shouldBe AttributeType.STRING
  }

  it should "ignore the input schema entirely (the plot output shape is fixed)" in {
    opDesc.valueColumn = "score"
    val fromEmpty = opDesc.getOutputSchemas(Map.empty[PortIdentity, Schema])
    val fromPopulated = opDesc.getOutputSchemas(
      Map(PortIdentity(7) -> Schema().add("anything", AttributeType.INTEGER))
    )
    fromEmpty shouldBe fromPopulated
  }

  // --- createPlotlyFigure: the argument list it assembles ---------------------

  "ECDFPlotOpDesc.createPlotlyFigure" should
    "emit only the value column when every optional setting is left at its default" in {
    opDesc.valueColumn = "score"
    val call = lineContaining(opDesc.createPlotlyFigure().plain, "px.ecdf(")

    decodeSiteCount(call) shouldBe 1
    // The payload, not just the marker: a decode site carrying colorColumn (empty by
    // default) would satisfy a marker-only check while plotting nothing.
    call should include(s"px.ecdf(table, x=$decodeSite('${b64("score")}')")
    call should not include "ecdfnorm"
    call should not include ", y="
    call should not include "color="
    call should not include "facet_col="
    call should not include "ecdfmode="
    call should not include "orientation="
    call should not include "markers=True"
    call should not include "marginal="
  }

  it should "request a cumulative sum by turning off normalization and passing y" in {
    opDesc.valueColumn = "score"
    opDesc.yAxisMode = "sum"
    val call = lineContaining(opDesc.createPlotlyFigure().plain, "px.ecdf(")

    call should include("ecdfnorm=None")
    call should include(s", y=$decodeSite('${b64("score")}')")
    // the value column is spliced twice: once as x, once as y
    decodeSiteCount(call) shouldBe 2
  }

  it should "turn off normalization without a y argument for raw counts" in {
    opDesc.valueColumn = "score"
    opDesc.yAxisMode = "count"
    val call = lineContaining(opDesc.createPlotlyFigure().plain, "px.ecdf(")

    call should include("ecdfnorm=None")
    call should not include ", y="
    decodeSiteCount(call) shouldBe 1
  }

  it should "omit ecdfmode, orientation and marginal when they hold their default values" in {
    opDesc.valueColumn = "score"
    opDesc.cdfMode = "standard"
    opDesc.orientation = "vertical"
    opDesc.marginal = "none"
    opDesc.showMarkers = false
    val call = lineContaining(opDesc.createPlotlyFigure().plain, "px.ecdf(")

    call should not include "ecdfmode="
    call should not include "orientation="
    call should not include "marginal="
    call should not include "markers="
  }

  it should "fall back to probability normalization when yAxisMode arrives null" in {
    // A saved workflow can carry "yAxisMode": null. The match must fall through to
    // its default arm and emit the probability form -- neither throwing nor turning
    // normalization off the way the explicit "count"/"sum" modes do.
    val json =
      """{"operatorType":"ECDFPlot","operatorID":"ECDFPlot-1","valueColumn":"score","yAxisMode":null}"""
    val d = objectMapper.readValue(json, classOf[LogicalOp]).asInstanceOf[ECDFPlotOpDesc]
    d.yAxisMode shouldBe null

    val call = lineContaining(d.createPlotlyFigure().plain, "px.ecdf(")
    call should not include "ecdfnorm"
    call should not include ", y="
    decodeSiteCount(call) shouldBe 1
  }

  // --- manipulateTable: the dropna column list -------------------------------

  "ECDFPlotOpDesc.manipulateTable" should
    "require only the value column when no optional column is configured" in {
    opDesc.valueColumn = "score"
    val required = lineContaining(opDesc.manipulateTable().plain, "required_cols = [")

    decodeSiteCount(required) shouldBe 1
    assert(carries(required, "score"))
  }

  it should "require the color and separate-by columns too when they are configured" in {
    opDesc.valueColumn = "score"
    opDesc.colorColumn = "group"
    opDesc.separateBy = "category"
    val required = lineContaining(opDesc.manipulateTable().plain, "required_cols = [")

    decodeSiteCount(required) shouldBe 3
    assert(carries(required, "score"))
    assert(carries(required, "group"))
    assert(carries(required, "category"))
  }

  it should "coerce the value column to numeric and drop the rows that fail" in {
    opDesc.valueColumn = "score"
    val table = opDesc.manipulateTable().plain

    table should include("pd.to_numeric(")
    table should include("errors='coerce'")
    table should include("inplace=True")
  }

  // --- generated code ---------------------------------------------------------

  "ECDFPlotOpDesc.generatePythonCode" should
    "wrap the figure in a table operator that guards both empty-table cases" in {
    opDesc.valueColumn = "score"
    val code = opDesc.generatePythonCode()

    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("def process_table")
    code should include("input table is empty.")
    code should include("no valid rows left after removing missing or non-numeric values.")
    code should include("plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)")
    code should include("yield {'html-content': html}")

    // Order matters, and a bag of unordered `include`s cannot see it. The cleaning
    // step has to be spliced BEFORE the figure so the plot is built on cleaned rows,
    // with the "no valid rows left" guard sitting between the two; swapping the two
    // splices would plot uncleaned data and leave that second guard dead.
    code should include("pd.to_numeric(")
    code should include("px.ecdf(")
    code.indexOf("pd.to_numeric(") should be < code.indexOf("px.ecdf(")
    code.indexOf("px.ecdf(") should be < code.indexOf("plotly.io.to_html(")
    code.indexOf("input table is empty.") should be <
      code.indexOf("no valid rows left after removing missing or non-numeric values.")
  }

  // --- JSON round-trip --------------------------------------------------------

  "ECDFPlotOpDesc" should "round-trip all of its config fields through the polymorphic base" in {
    opDesc.valueColumn = "score"
    opDesc.colorColumn = "group"
    opDesc.separateBy = "category"
    opDesc.yAxisMode = "sum"
    opDesc.cdfMode = "complementary"
    opDesc.orientation = "horizontal"
    opDesc.showMarkers = true
    opDesc.marginal = "rug"

    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(opDesc), classOf[LogicalOp])

    restored shouldBe a[ECDFPlotOpDesc]
    val d = restored.asInstanceOf[ECDFPlotOpDesc]
    d.valueColumn shouldBe "score"
    d.colorColumn shouldBe "group"
    d.separateBy shouldBe "category"
    d.yAxisMode shouldBe "sum"
    d.cdfMode shouldBe "complementary"
    d.orientation shouldBe "horizontal"
    d.showMarkers shouldBe true
    d.marginal shouldBe "rug"
  }

  it should "keep its documented defaults when deserialized from a minimal JSON payload" in {
    val json =
      """{"operatorType":"ECDFPlot","operatorID":"ECDFPlot-1","valueColumn":"score"}"""
    val d = objectMapper.readValue(json, classOf[LogicalOp]).asInstanceOf[ECDFPlotOpDesc]

    d.valueColumn shouldBe "score"
    d.colorColumn shouldBe ""
    d.separateBy shouldBe ""
    d.yAxisMode shouldBe "probability"
    d.cdfMode shouldBe "standard"
    d.orientation shouldBe "vertical"
    d.showMarkers shouldBe false
    d.marginal shouldBe "none"
  }
}
