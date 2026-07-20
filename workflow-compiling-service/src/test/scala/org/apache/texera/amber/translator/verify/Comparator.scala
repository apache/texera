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
object Comparator extends LazyLogging {

  // Resource path is absolute (leading slash) so getResourceAsStream resolves
  // against the classpath root regardless of caller's package.
  private val ScriptResourcePath = "/python/compare.py"

  def assertEqual(
      actual: Path,
      expected: Path,
      orderSensitive: Boolean = true,
      ignoreColumns: Seq[String] = Seq.empty,
      modelColumns: Seq[String] = Seq.empty,
      probePath: Option[Path] = None,
      pythonExe: String = resolvePython()
  ): Unit = {
    val (exit, stdout, stderr) =
      compare(actual, expected, orderSensitive, ignoreColumns, modelColumns, probePath, pythonExe)
    if (exit != 0) {
      throw new ComparatorMismatchException(
        actual = actual,
        expected = expected,
        exitCode = exit,
        stdout = stdout,
        stderr = stderr
      )
    }
  }

  // Prefer a pooled persistent worker (imports pandas once via `compare.py
  // --serve`, so the ~214 ms import isn't repaid per comparison — the diff
  // itself is ~ms). A rare hard worker crash falls back to the one-shot CLI so
  // behavior is never worse than the original path. Both invoke the same
  // `_run_comparison`, so results are identical.
  private def compare(
      actual: Path,
      expected: Path,
      orderSensitive: Boolean,
      ignoreColumns: Seq[String],
      modelColumns: Seq[String],
      probePath: Option[Path],
      pythonExe: String
  ): (Int, String, String) = {
    if (PythonWorkerPool.enabled) {
      try {
        val req = objectMapper.createObjectNode()
        req.put("actual", actual.toString)
        req.put("expected", expected.toString)
        req.put("unordered", !orderSensitive)
        val ignoreArr = req.putArray("ignoreCols")
        ignoreColumns.foreach(ignoreArr.add)
        val modelArr = req.putArray("modelCols")
        modelColumns.foreach(modelArr.add)
        // --probe only applies with --model-cols (mirrors the CLI's guard).
        probePath.filter(_ => modelColumns.nonEmpty) match {
          case Some(p) => req.put("probe", p.toString)
          case None    => req.putNull("probe")
        }
        val o = PythonWorkerPool.run(ScriptResourcePath, Seq("--serve"), pythonExe, req)
        return (o.exit, o.stdout, o.stderr)
      } catch {
        case e: PythonWorkerPool.WorkerDiedException =>
          logger.warn(
            s"Comparator worker unavailable; falling back to one-shot CLI: ${e.getMessage}"
          )
      }
    }
    runCli(actual, expected, orderSensitive, ignoreColumns, modelColumns, probePath, pythonExe)
  }

  // Original one-subprocess-per-comparison CLI path. Retained as the fallback
  // and as the behavior selected by VERIFY_PYTHON_WORKER=0.
  private def runCli(
      actual: Path,
      expected: Path,
      orderSensitive: Boolean,
      ignoreColumns: Seq[String],
      modelColumns: Seq[String],
      probePath: Option[Path],
      pythonExe: String
  ): (Int, String, String) = {
    val scriptPath = extractScript()
    val outBuf = ArrayBuffer.empty[String]
    val errBuf = ArrayBuffer.empty[String]
    val procLogger = ProcessLogger(line => outBuf += line, line => errBuf += line)
    // --unordered tells compare.py to lex-sort both DataFrames by all columns
    // before assert_frame_equal — needed for set-semantics ops whose JVM
    // emission order doesn't match the pandas equivalent. Default stays
    // positional so deterministic-order ops still catch row-order regressions.
    // --ignore-cols drops opaque columns whose value isn't compared.
    // --model-cols + --probe compare a model column by behavior: unpickle both
    // sides and assert their predictions on the probe feature set match (two
    // independently-trained models are functionally equal but not bit-equal).
    val baseArgs = Seq(pythonExe, scriptPath.toString)
    val flagArgs = if (!orderSensitive) Seq("--unordered") else Seq.empty
    val ignoreArgs =
      if (ignoreColumns.nonEmpty) Seq("--ignore-cols", ignoreColumns.mkString(",")) else Seq.empty
    val modelArgs =
      if (modelColumns.nonEmpty) Seq("--model-cols", modelColumns.mkString(",")) else Seq.empty
    val probeArgs =
      probePath
        .filter(_ => modelColumns.nonEmpty)
        .map(p => Seq("--probe", p.toString))
        .getOrElse(Seq.empty)
    val cmd =
      baseArgs ++ flagArgs ++ ignoreArgs ++ modelArgs ++ probeArgs ++ Seq(
        actual.toString,
        expected.toString
      )
    val exit = Process(cmd).!(procLogger)
    (exit, outBuf.mkString("\n"), errBuf.mkString("\n"))
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
