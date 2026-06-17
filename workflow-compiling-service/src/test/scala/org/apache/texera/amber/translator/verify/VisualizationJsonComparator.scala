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

import java.nio.file.{Files, Path, StandardCopyOption}
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

object VisualizationJsonComparator {

  private val ScriptResourcePath = "/python/compare_plotly_json.py"

  def assertEqual(
      actualVisualizationJsonl: Path,
      expectedPlotlyJson: Path,
      pythonExe: String = resolvePython()
  ): Unit = {
    val scriptPath = extractScript()
    val outBuf = ArrayBuffer.empty[String]
    val errBuf = ArrayBuffer.empty[String]
    val logger = ProcessLogger(line => outBuf += line, line => errBuf += line)

    val exit = Process(
      Seq(
        pythonExe,
        scriptPath.toString,
        actualVisualizationJsonl.toString,
        expectedPlotlyJson.toString
      )
    ).!(logger)

    if (exit != 0) {
      throw new VisualizationJsonMismatchException(
        actual = actualVisualizationJsonl,
        expected = expectedPlotlyJson,
        exitCode = exit,
        stdout = outBuf.mkString("\n"),
        stderr = errBuf.mkString("\n")
      )
    }
  }

  private def extractScript(): Path = {
    val stream = getClass.getResourceAsStream(ScriptResourcePath)
    require(stream != null, s"compare_plotly_json.py not found at $ScriptResourcePath")
    try {
      val tmp = Files.createTempFile("compare-plotly-json-", ".py")
      Files.copy(stream, tmp, StandardCopyOption.REPLACE_EXISTING)
      tmp.toFile.deleteOnExit()
      tmp
    } finally stream.close()
  }

  private def resolvePython(): String =
    sys.env.get("UDF_PYTHON_PATH").filter(_.nonEmpty).getOrElse("python3.12")
}

final class VisualizationJsonMismatchException(
    val actual: Path,
    val expected: Path,
    val exitCode: Int,
    val stdout: String,
    val stderr: String
) extends RuntimeException(
      s"""Visualization JSON mismatch (compare_plotly_json.py exit $exitCode):
         |  actual:   $actual
         |  expected: $expected
         |--- stderr ---
         |$stderr""".stripMargin
    )
