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

  def assertEqual(actualVisualizationJsonl: Path, expectedHtmlFile: Path): Unit = {
    val actual = readActualHtml(actualVisualizationJsonl)
    val expected = new String(Files.readAllBytes(expectedHtmlFile), StandardCharsets.UTF_8)

    if (actual != expected) {
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
