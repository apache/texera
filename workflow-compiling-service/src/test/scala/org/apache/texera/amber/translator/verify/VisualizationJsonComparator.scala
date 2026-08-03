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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.python.PythonWorkerPool

import java.nio.file.{Files, Path, StandardCopyOption}
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

/**
  * Compares the Plotly figure the two paths render, via `compare.py`'s
  * `--plotly` mode.
  *
  * Shares that script — and therefore [[Comparator]]'s pool — rather than
  * carrying one of its own: a worker is bound to the script it was launched
  * with, so a separate script would mean a separate pool of interpreters for
  * what is the same job, comparing one operator's two outputs. One comparison
  * pool serves both output shapes.
  */
object VisualizationJsonComparator extends LazyLogging {

  private val ScriptResourcePath = "/python/compare.py"

  def assertEqual(
      actualVisualizationJsonl: Path,
      expectedPlotlyJson: Path,
      pythonExe: String = resolvePython()
  ): Unit = {
    val (exit, stdout, stderr) =
      compare(actualVisualizationJsonl, expectedPlotlyJson, pythonExe)
    if (exit != 0) {
      throw new VisualizationJsonMismatchException(
        actual = actualVisualizationJsonl,
        expected = expectedPlotlyJson,
        exitCode = exit,
        stdout = stdout,
        stderr = stderr
      )
    }
  }

  // Pooled worker first, one-shot CLI as the fallback and as the behavior
  // selected by TEXERA_TEST_PYTHON_WORKER=0. Both run the same
  // `_run_plotly_comparison`, so results are identical.
  private def compare(actual: Path, expected: Path, pythonExe: String): (Int, String, String) = {
    if (PythonWorkerPool.enabled) {
      try {
        val req = objectMapper.createObjectNode()
        req.put("kind", "plotly")
        req.put("actual", actual.toString)
        req.put("expected", expected.toString)
        val o = PythonWorkerPool.run(ScriptResourcePath, Seq("--serve"), pythonExe, req)
        return (o.exit, o.stdout, o.stderr)
      } catch {
        case e: PythonWorkerPool.WorkerDiedException =>
          logger.warn(
            s"Comparator worker unavailable; falling back to one-shot CLI: ${e.getMessage}"
          )
      }
    }
    runCli(actual, expected, pythonExe)
  }

  private def runCli(actual: Path, expected: Path, pythonExe: String): (Int, String, String) = {
    val scriptPath = extractScript()
    val outBuf = ArrayBuffer.empty[String]
    val errBuf = ArrayBuffer.empty[String]
    val processLogger = ProcessLogger(line => outBuf += line, line => errBuf += line)

    val exit = Process(
      Seq(pythonExe, scriptPath.toString, "--plotly", actual.toString, expected.toString)
    ).!(processLogger)
    (exit, outBuf.mkString("\n"), errBuf.mkString("\n"))
  }

  private def extractScript(): Path = {
    val stream = getClass.getResourceAsStream(ScriptResourcePath)
    require(stream != null, s"compare.py not found at $ScriptResourcePath")
    try {
      val tmp = Files.createTempFile("compare-", ".py")
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
      s"""Visualization JSON mismatch (compare.py --plotly exit $exitCode):
         |  actual:   $actual
         |  expected: $expected
         |--- stderr ---
         |$stderr""".stripMargin
    )
