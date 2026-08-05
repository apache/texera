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

object VisualizationHtmlComparator {

  /** A pandas Styler namespaces its CSS with a uuid drawn per Styler instance, so
    * the same table rendered twice differs in every `id=` and every selector even
    * though the markup is identical. The uuid carries no information about the
    * table — it only keeps two tables on one page from colliding — so it is
    * normalized away before comparing. Only the random prefix is replaced: the
    * `_row0_col0` suffix that identifies the cell stays, so a genuine structural
    * difference still fails.
    */
  private val StylerUuid = "T_[0-9a-f]+".r

  private def normalize(html: String): String = StylerUuid.replaceAllIn(html, "T_uuid")

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
