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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.translator.verify.tags.IntegrationTest
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** Tagged @IntegrationTest with the rest of the specs that fork Python: the
  * comparison itself is `compare.py --plotly`.
  */
@IntegrationTest
class VisualizationJsonComparatorSpec extends AnyFlatSpec with Matchers {

  private val dir: Path = Files.createTempDirectory("visualization-json-comparator-spec-")

  /** One figure, as plotly writes it into a page. */
  private def figure(value: Int): String =
    s"""<div><script>Plotly.newPlot("div$value", """ +
      s"""[{"type": "indicator", "value": $value}], {"height": 150}, {"responsive": true})""" +
      "</script></div>"

  /** The runtime path's side: the whole page as one `html-content` JSONL row,
    * which is how an operator that draws a figure per row hands them over.
    */
  private def writeActual(name: String, figures: Int*): Path = {
    val node = objectMapper.createObjectNode()
    node.put("html-content", figures.map(figure).mkString("<div>", "", "</div>"))
    val p = dir.resolve(name)
    Files.write(p, objectMapper.writeValueAsBytes(node))
    p
  }

  /** The standalone path's side: its page, holding every figure it drew. */
  private def writePage(name: String, figures: Int*): Path = {
    val p = dir.resolve(name)
    Files.write(p, figures.map(figure).mkString.getBytes(StandardCharsets.UTF_8))
    p
  }

  /** The standalone path's other side: the single figure `write_json` leaves. */
  private def writeFigureJson(name: String, value: Int): Path = {
    val p = dir.resolve(name)
    val json = s"""{"data": [{"type": "indicator", "value": $value}], """ +
      """"layout": {"height": 150}}"""
    Files.write(p, json.getBytes(StandardCharsets.UTF_8))
    p
  }

  "VisualizationJsonComparator" should "accept the one figure the script wrote as JSON" in {
    val actual = writeActual("single-actual.jsonl", 1)
    val expected = writeFigureJson("single-expected.json", 1)
    noException should be thrownBy VisualizationJsonComparator.assertEqual(actual, expected)
  }

  it should "accept a run whose every figure the other path drew the same" in {
    val actual = writeActual("multi-actual.jsonl", 1, 2, 3)
    val expected = writePage("multi-expected.html", 1, 2, 3)
    noException should be thrownBy VisualizationJsonComparator.assertEqual(actual, expected)
  }

  // The one a comparison that reads the first figure and stops would call a
  // match, and the reason it reads them all: an operator drawing a figure per
  // input row agrees on the first long after it has stopped agreeing.
  it should "reject a second figure the other path drew differently" in {
    val actual = writeActual("second-actual.jsonl", 1, 2)
    val expected = writePage("second-expected.html", 1, 7)
    val thrown = the[VisualizationJsonMismatchException] thrownBy VisualizationJsonComparator
      .assertEqual(actual, expected)
    thrown.stderr should include("\"value\": 7")
  }

  it should "reject a figure the other path did not draw at all" in {
    val actual = writeActual("count-actual.jsonl", 1, 2)
    val expected = writePage("count-expected.html", 1)
    val thrown = the[VisualizationJsonMismatchException] thrownBy VisualizationJsonComparator
      .assertEqual(actual, expected)
    // The counts, so the failure reads as a missing figure rather than as two
    // long dumps to count out by hand.
    thrown.stderr should include("(2 drawn, 1 expected)")
  }
}
