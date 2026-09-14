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

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

// Untagged: the comparator reads two files and compares strings, with no Python
// to shell out to, so this belongs in the plain unit job.
class VisualizationHtmlComparatorSpec extends AnyFlatSpec with Matchers {

  private val dir: Path = Files.createTempDirectory("visualization-html-comparator-spec-")

  /** The runtime path's side: the markup as one `html-content` field of a JSONL row. */
  private def writeActual(name: String, html: String): Path = {
    val node = objectMapper.createObjectNode()
    node.put("html-content", html)
    val p = dir.resolve(name)
    Files.write(p, objectMapper.writeValueAsBytes(node))
    p
  }

  /** The standalone path's side: the markup as the script wrote it to a file. */
  private def writeExpected(name: String, html: String): Path = {
    val p = dir.resolve(name)
    Files.write(p, html.getBytes(StandardCharsets.UTF_8))
    p
  }

  private val page = "<html>\n<body>\n<p>chart</p>\n</body>\n</html>\n"

  "VisualizationHtmlComparator" should "accept two sides that differ only in line endings" in {
    val actual = writeActual("crlf-actual.jsonl", page)
    val expected = writeExpected("crlf-expected.html", page.replace("\n", "\r\n"))
    noException should be thrownBy VisualizationHtmlComparator.assertEqual(actual, expected)
  }

  /** The two places a Styler writes its uuid: the CSS selector and the `id` attribute. */
  private def styledCell(uuid: String, value: String) =
    s"""<style>#${uuid}_row0_col0 { color: red; }</style>""" +
      s"""<td id="${uuid}_row0_col0">$value</td>"""

  it should "accept two sides that differ only in a Styler uuid" in {
    val actual = writeActual("styler-actual.jsonl", styledCell("T_a1b2c3", "1"))
    val expected = writeExpected("styler-expected.html", styledCell("T_9f8e7d", "1"))
    noException should be thrownBy VisualizationHtmlComparator.assertEqual(actual, expected)
  }

  // A Styler does not escape a cell, so a value can read exactly like a uuid,
  // `#` and all. What the table says is compared wherever it lands.
  Seq("T_dead" -> "T_beef", "#T_dead" -> "#T_beef").foreach {
    case (oneValue, otherValue) =>
      it should s"still reject two sides whose cell reads $oneValue against $otherValue" in {
        val name = oneValue.filter(_.isLetterOrDigit)
        val actual = writeActual(s"cell-$name-actual.jsonl", styledCell("T_a1b2c3", oneValue))
        val expected =
          writeExpected(s"cell-$name-expected.html", styledCell("T_a1b2c3", otherValue))
        a[VisualizationHtmlMismatchException] should be thrownBy VisualizationHtmlComparator
          .assertEqual(actual, expected)
      }
  }

  it should "still reject markup that differs in more than its line endings" in {
    val actual = writeActual("differ-actual.jsonl", page)
    val expected =
      writeExpected("differ-expected.html", page.replace("chart", "table").replace("\n", "\r\n"))
    a[VisualizationHtmlMismatchException] should be thrownBy VisualizationHtmlComparator
      .assertEqual(actual, expected)
  }
}
