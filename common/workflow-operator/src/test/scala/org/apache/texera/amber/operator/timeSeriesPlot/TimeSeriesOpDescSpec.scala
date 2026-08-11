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

package org.apache.texera.amber.operator.timeSeriesPlot

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorMetadataGenerator}
import org.apache.texera.amber.operator.visualization.timeSeriesplot.TimeSeriesOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.jdk.CollectionConverters._

class TimeSeriesOpDescSpec extends AnyFunSuite {

  test("generatePythonCode returns non-empty python code") {
    val op = new TimeSeriesOpDesc

    // set minimal required fields
    op.timeColumn = "date"
    op.valueColumn = "value"
    op.CategoryColumn = "cat"
    op.facetColumn = "facet"
    op.plotType = "line"
    op.showRangeSlider = false

    val py = op.generatePythonCode()

    assert(py.nonEmpty)
    assert(py.contains("class ProcessTableOperator"))
    assert(py.contains("def process_table"))
  }

  // --- helpers ---------------------------------------------------------------

  /** Column names reach the emitted Python base64-encoded inside a decode call. */
  private val decodeSite = "self.decode_python_template"

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def decodeSiteCount(s: String): Int =
    s.split(java.util.regex.Pattern.quote(decodeSite), -1).length - 1

  private def lineContaining(code: String, marker: String): String =
    code.linesIterator
      .find(_.contains(marker))
      .getOrElse(fail(s"no line containing '$marker' in:\n$code"))

  /**
    * The bracketed `dropna(subset=[...])` list only. The surrounding line also
    * carries a `sort_values(by=...)` splice, so it cannot be counted wholesale.
    * Base64 payloads never contain ']', so the first ']' closes the list.
    */
  private def dropnaSubset(code: String): String = {
    val line = lineContaining(code, "table.dropna(subset=[")
    val start = line.indexOf("subset=[") + "subset=[".length
    line.substring(start, line.indexOf(']', start))
  }

  /** Minimally configured operator: both optional columns left at "No Selection". */
  private def minimalOp(): TimeSeriesOpDesc = {
    val op = new TimeSeriesOpDesc
    op.timeColumn = "date"
    op.valueColumn = "value"
    op
  }

  // --- operator metadata ------------------------------------------------------

  test("operatorInfo advertises the plot name, basic viz group, and a 1-in/1-out shape") {
    val info = minimalOp().operatorInfo
    assert(info.userFriendlyName == "Time Series Plot")
    assert(info.operatorDescription == "Visualize trends and patterns over time.")
    assert(info.operatorGroupName == OperatorGroupConstants.VISUALIZATION_BASIC_GROUP)
    assert(info.inputPorts.length == 1)
    assert(info.outputPorts.length == 1)
  }

  test("getOutputSchemas emits one html-content STRING column on the declared output port") {
    val op = minimalOp()
    val inputPort = op.operatorInfo.inputPorts.head.id
    val out = op.getOutputSchemas(
      Map(inputPort -> Schema().add("date", AttributeType.TIMESTAMP))
    )

    assert(out.keySet == Set(op.operatorInfo.outputPorts.head.id))
    val schema = out(op.operatorInfo.outputPorts.head.id)
    assert(schema.getAttributeNames == List("html-content"))
    assert(schema.getAttribute("html-content").getType == AttributeType.STRING)
  }

  test("getOutputSchemas ignores the input schema (the plot output shape is fixed)") {
    val op = minimalOp()
    val fromEmpty = op.getOutputSchemas(Map.empty[PortIdentity, Schema])
    val fromPopulated =
      op.getOutputSchemas(Map(PortIdentity(3) -> Schema().add("x", AttributeType.INTEGER)))
    assert(fromEmpty == fromPopulated)
  }

  // --- optional columns -------------------------------------------------------

  test("the default 'No Selection' optional columns add neither color, facet, nor dropna entries") {
    val py = minimalOp().generatePythonCode()

    assert(!py.contains(", color="))
    assert(!py.contains(", facet_col="))
    // only timeColumn and valueColumn are required to be non-null
    assert(decodeSiteCount(dropnaSubset(py)) == 2)
  }

  test("a configured category column adds a color argument and a dropna entry") {
    val op = minimalOp()
    op.CategoryColumn = "region"
    val py = op.generatePythonCode()

    assert(py.contains(", color="))
    assert(!py.contains(", facet_col="))
    assert(py.contains(b64("region")))
    assert(decodeSiteCount(dropnaSubset(py)) == 3)
  }

  test("a configured facet column adds a facet_col argument and a dropna entry") {
    val op = minimalOp()
    op.CategoryColumn = "region"
    op.facetColumn = "store"
    val py = op.generatePythonCode()

    assert(py.contains(", color="))
    assert(py.contains(", facet_col="))
    assert(py.contains(b64("store")))
    assert(decodeSiteCount(dropnaSubset(py)) == 4)
  }

  // --- plot type and range slider ---------------------------------------------

  test("plotType selects px.line by default and px.area when asked for an area chart") {
    assert(minimalOp().generatePythonCode().contains("fig = px.line(table"))

    val areaOp = minimalOp()
    areaOp.plotType = "area"
    val areaPy = areaOp.generatePythonCode()
    assert(areaPy.contains("fig = px.area(table"))
    assert(!areaPy.contains("px.line("))

    // any unrecognized plot type falls back to a line chart
    val oddOp = minimalOp()
    oddOp.plotType = "bar"
    assert(oddOp.generatePythonCode().contains("fig = px.line(table"))
  }

  test("showRangeSlider gates the rangeslider_visible call") {
    val off = minimalOp()
    off.showRangeSlider = false
    val offPy = off.generatePythonCode()
    assert(offPy.contains("if False:"))
    assert(!offPy.contains("if True:"))

    val on = minimalOp()
    on.showRangeSlider = true
    val onPy = on.generatePythonCode()
    assert(onPy.contains("if True:"))
    assert(onPy.contains("fig.update_xaxes(rangeslider_visible=True)"))
  }

  test("showRangeSlider defaults to false on a freshly constructed descriptor") {
    assert(!new TimeSeriesOpDesc().showRangeSlider)
  }

  test("the generated schema offers Plot Type as exactly the two values the code reads") {
    // The property is declared under the name "line", which is what saved workflows carry.
    val plotType = OperatorMetadataGenerator
      .generateOperatorJsonSchema(classOf[TimeSeriesOpDesc])
      .path("properties")
      .path("line")

    assert(plotType.path("enum").elements().asScala.map(_.asText()).toList == List("line", "area"))
    // the schema default is what the descriptor itself starts at, so an untouched form
    // and an untouched descriptor draw the same chart
    assert(plotType.path("default").asText() == "line")
    assert(plotType.path("default").asText() == new TimeSeriesOpDesc().plotType)
  }

  // --- generated code shape ----------------------------------------------------

  test("generated code coerces the time column, sorts by it, and guards both empty cases") {
    val py = minimalOp().generatePythonCode()

    assert(py.contains("pd.to_datetime("))
    assert(py.contains("errors='coerce'"))
    assert(py.contains(".sort_values(by="))
    assert(py.contains("Input table is empty."))
    assert(py.contains("Table became empty after filtering."))
    assert(py.contains("except Exception as e:"))
    assert(py.contains("plotly.io.to_html(fig, include_plotlyjs='cdn', full_html=False)"))
    // column names are encoded, never spliced raw
    assert(py.contains(b64("date")) && py.contains(b64("value")))
  }

  // --- column type rules --------------------------------------------------------

  /** The rule the property editor reads to decide which columns a picker offers. */
  private def attributeTypeRule(property: String): Set[String] =
    OperatorMetadataGenerator
      .generateOperatorJsonSchema(classOf[TimeSeriesOpDesc])
      .path("attributeTypeRules")
      .path(property)
      .path("enum")
      .elements()
      .asScala
      .map(_.asText())
      .toSet

  test("the time column accepts a date column stored as text, not only a timestamp") {
    // A CSV source hands its dates over as STRING, and pd.to_datetime parses them,
    // so rejecting the type would invalidate workflows that plot today.
    assert(attributeTypeRule("timeColumn") == Set("timestamp", "string"))
  }

  test("the value column is restricted to the numeric types") {
    assert(attributeTypeRule("valueColumn") == Set("integer", "long", "double"))
  }

  test("text the parser cannot read is dropped and reported rather than raised") {
    // The type rule admits any string, so unparseable text still reaches the
    // generated code; coercion turns it into NaT, dropna removes the row, and an
    // input whose every row goes that way ends in a message instead of a traceback.
    val py = minimalOp().generatePythonCode()

    assert(py.contains("errors='coerce'"))
    assert(dropnaSubset(py).contains(b64("date")))
    assert(py.contains("Table became empty after filtering."))
  }

  // --- JSON round-trip ---------------------------------------------------------

  test("the descriptor round-trips all of its config fields through the polymorphic base") {
    val op = new TimeSeriesOpDesc
    op.timeColumn = "date"
    op.valueColumn = "sales"
    op.CategoryColumn = "region"
    op.facetColumn = "store"
    op.plotType = "area"
    op.showRangeSlider = true

    val restored = objectMapper.readValue(objectMapper.writeValueAsString(op), classOf[LogicalOp])

    assert(restored.isInstanceOf[TimeSeriesOpDesc])
    val d = restored.asInstanceOf[TimeSeriesOpDesc]
    assert(d.timeColumn == "date")
    assert(d.valueColumn == "sales")
    assert(d.CategoryColumn == "region")
    assert(d.facetColumn == "store")
    assert(d.plotType == "area")
    assert(d.showRangeSlider)
  }

  test("a minimal JSON payload deserializes with the documented defaults") {
    val json =
      """{"operatorType":"TimeSeriesPlot","operatorID":"TimeSeriesPlot-1",""" +
        """"timeColumn":"date","valueColumn":"sales"}"""
    val d = objectMapper.readValue(json, classOf[LogicalOp]).asInstanceOf[TimeSeriesOpDesc]

    assert(d.timeColumn == "date")
    assert(d.valueColumn == "sales")
    assert(d.CategoryColumn == "No Selection")
    assert(d.facetColumn == "No Selection")
    assert(d.plotType == "line")
    assert(!d.showRangeSlider)
  }
}
