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

/**
  * Runs the Python comparator (`compare.py`) on two JSONL files emitted by
  * [[OpExecHarness]] (actual) and [[StandaloneRunner]] (expected). The
  * comparator uses `pandas.testing.assert_frame_equal` with `check_like=True`
  * and `check_dtype=False` so row/column-order differences and the
  * pandas-int64/float64 coercion that happens when JSONL round-trips through
  * `pd.read_json` don't trigger false negatives. Float tolerance: `rtol=1e-5`.
  *
  * Throws [[ComparatorMismatchException]] on any non-zero exit code (the
  * pandas diff is in `stderr` on the exception). Successful comparisons
  * return unit.
  *
  * Python resolution mirrors [[StandaloneRunner.resolvePython]]:
  * `UDF_PYTHON_PATH` env var first, else `python3.12` on PATH.
  */
object Comparator {

  // Resource path is absolute (leading slash) so getResourceAsStream resolves
  // against the classpath root regardless of caller's package.
  private val ScriptResourcePath = "/python/compare.py"

  def assertEqual(
      actual: Path,
      expected: Path,
      pythonExe: String = resolvePython()
  ): Unit = {
    val scriptPath = extractScript()
    val outBuf = ArrayBuffer.empty[String]
    val errBuf = ArrayBuffer.empty[String]
    val procLogger = ProcessLogger(line => outBuf += line, line => errBuf += line)
    val exit = Process(
      Seq(pythonExe, scriptPath.toString, actual.toString, expected.toString)
    ).!(procLogger)

    if (exit != 0) {
      throw new ComparatorMismatchException(
        actual = actual,
        expected = expected,
        exitCode = exit,
        stdout = outBuf.mkString("\n"),
        stderr = errBuf.mkString("\n")
      )
    }
  }

  // Resources may live inside a jar at runtime; copy to a temp file so Python
  // can exec it. deleteOnExit so test runs don't accumulate /tmp clutter.
  private def extractScript(): Path = {
    val stream = getClass.getResourceAsStream(ScriptResourcePath)
    require(
      stream != null,
      s"compare.py not found on classpath at $ScriptResourcePath"
    )
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

final class ComparatorMismatchException(
    val actual: Path,
    val expected: Path,
    val exitCode: Int,
    val stdout: String,
    val stderr: String
) extends RuntimeException(
      s"""DataFrame mismatch (compare.py exit $exitCode):
         |  actual:   $actual
         |  expected: $expected
         |--- stderr ---
         |$stderr""".stripMargin
    )
