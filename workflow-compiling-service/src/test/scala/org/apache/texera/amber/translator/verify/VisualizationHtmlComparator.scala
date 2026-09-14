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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.util.matching.Regex

object VisualizationHtmlComparator {

  /** A pandas Styler namespaces its CSS with a uuid drawn per Styler instance, so
    * the same table rendered twice differs in every `id=` and every selector even
    * though the markup is identical. The uuid carries no information about the
    * table, it only keeps two tables on one page from colliding, so it is
    * normalized away before comparing.
    *
    * It is normalized only where the markup uses it, never in what the table
    * says. A Styler writes it in the `#` selectors inside its `<style>` element
    * and in the `id` attribute of the table and its cells, so the rewrite visits
    * those two places and leaves text content alone. A cell reading `#T_dead` is
    * the table's own value and still has to match the other side, and the
    * `_row0_col0` suffix stays, so a real difference still fails.
    */
  private val StyleElement = "(?s)<style\\b.*?</style>".r
  private val Tag = "<[^>]*>".r
  private val SelectorUuid = "(?<=#)T_[0-9a-f]+".r
  private val IdUuid = """(?<=id=")T_[0-9a-f]+""".r

  /** The standalone script writes its page with Python's text mode, which on Windows
    * turns every newline into CRLF, while the runtime path carries the same markup
    * through JSONL untouched and so keeps LF. The line ending is the platform writing
    * the file rather than anything the operator chose, so it is normalized away too.
    */
  private val LineEnding = "\r\n|\r".r

  private def normalize(html: String): String = {
    val page = LineEnding.replaceAllIn(html, "\n")
    val styled = StyleElement.replaceAllIn(page, within(SelectorUuid))
    Tag.replaceAllIn(styled, within(IdUuid))
  }

  /** The same page with `uuid` normalized inside the region that matched. */
  private def within(uuid: Regex): Regex.Match => String =
    region => Regex.quoteReplacement(uuid.replaceAllIn(region.matched, "T_uuid"))

  def assertEqual(actualVisualizationJsonl: Path, expectedHtmlFile: Path): Unit = {
    val actual = readActualHtml(actualVisualizationJsonl)
    val expected = new String(Files.readAllBytes(expectedHtmlFile), StandardCharsets.UTF_8)

    if (normalize(actual) != normalize(expected)) {
      throw new VisualizationHtmlMismatchException(
        actual = actualVisualizationJsonl,
        expected = expectedHtmlFile,
        actualHtml = actual,
        expectedHtml = expected
      )
    }
  }

  private def readActualHtml(path: Path): String = {
    val line = Files
      .readAllLines(path, StandardCharsets.UTF_8)
      .stream()
      .filter(_.trim.nonEmpty)
      .findFirst()
      .orElseThrow(() => new AssertionError(s"$path is empty"))

    val node = objectMapper.readTree(line)
    val htmlNode = node.get("html-content")
    if (htmlNode == null || htmlNode.isNull) {
      throw new AssertionError(s"$path has no html-content field")
    }
    htmlNode.asText()
  }
}

final class VisualizationHtmlMismatchException(
    val actual: Path,
    val expected: Path,
    val actualHtml: String,
    val expectedHtml: String
) extends RuntimeException(
      s"""Visualization HTML mismatch:
         |  actual:   $actual
         |  expected: $expected
         |--- actual html ---
         |$actualHtml
         |--- expected html ---
         |$expectedHtml""".stripMargin
    )
