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
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

/**
  * Executes the Python code an OpDesc's [[StandaloneCodeGenerator]] emits and
  * captures its DataFrame outputs as JSONL files (compatible with
  * [[TupleIO]]'s sidecar-schema format on the comparison side).
  *
  * Wraps the operator's raw generated code with:
  *
  *   ── prologue ──────────────────────────────────────────────
  *     in1df = pd.read_json("input_port_0.jsonl", lines=True)
  *     in2df = pd.read_json("input_port_1.jsonl", lines=True)
  *     ...
  *   ── operator body (verbatim from generateStandaloneCode) ──
  *     out1df = in1df[in1df["age"] > 18]
  *   ── epilogue ─────────────────────────────────────────────
  *     out1df.to_json("output_port_0.jsonl", orient='records', lines=True)
  *     ...
  *
  * Port indexing matches the placeholder convention used by the translator:
  * `inNdf`/`outNdf` is 1-based and corresponds to the operator's N-th external
  * input/output port in declaration order. The harness key (a 1-based Int) is
  * what the placeholder uses; the caller is responsible for ordering inputs
  * the same way the operator's `generateStandaloneCode()` expects.
  *
  * The subprocess inherits the caller's environment so the Python interpreter
  * picks up whatever pandas/plotly the test fixture installed.
  */
object StandaloneRunner extends LazyLogging {

  /**
    * @param outputs paths to the per-port output JSONL files. Empty map iff
    *                the operator's `producesDataFrame()` returned false
    *                (visualizations, etc.) — caller handles those separately.
    * @param stdout  raw subprocess stdout (useful for failure diagnostics)
    * @param stderr  raw subprocess stderr
    */
  final case class Result(outputs: Map[Int, Path], stdout: String, stderr: String)

  /**
    * Generate, write, and execute the standalone Python script for `opDesc`.
    *
    * @param opDesc must mix in [[StandaloneCodeGenerator]]; otherwise we throw
    *               since there's nothing to test.
    * @param inputs map from 1-based port index → JSONL fixture path. The
    *               script reads each into `inNdf`.
    * @param outputPortCount how many `outNdf` variables the operator declares.
    *                Caller derives this from the OpDesc's output ports.
    * @param workDir directory used for the generated `script.py` and output
    *                JSONL files. Created if missing.
    * @param pythonExe path to the Python 3.12 interpreter. Defaults to
    *                  the env var `UDF_PYTHON_PATH`, then `python3.12`, then
    *                  `python3`. The same fallback chain used by the rest of
    *                  the Texera test suite for Python-backed operators.
    */
  def run(
      opDesc: LogicalOp,
      inputs: Map[Int, Path],
      outputPortCount: Int,
      workDir: Path,
      pythonExe: String = resolvePython()
  ): Result = {
    val gen = opDesc match {
      case g: StandaloneCodeGenerator => g
      case other =>
        throw new IllegalArgumentException(
          s"OpDesc ${other.getClass.getSimpleName} does not implement " +
            s"StandaloneCodeGenerator; nothing to verify"
        )
    }

    Files.createDirectories(workDir)
    val scriptPath = workDir.resolve("script.py")
    val outputPaths: Map[Int, Path] =
      if (gen.producesDataFrame())
        (1 to outputPortCount).map(i => i -> workDir.resolve(s"output_port_${i - 1}.jsonl")).toMap
      else Map.empty

    val source = renderScript(gen.generateStandaloneCode(), inputs, outputPaths)
    Files.write(scriptPath, source.getBytes(StandardCharsets.UTF_8))

    // Capture stdout/stderr separately. ProcessLogger's append is called from
    // the subprocess's I/O thread, so we collect into ArrayBuffer (thread-safe
    // append is fine for this serial use) and join at the end.
    val outBuf = ArrayBuffer.empty[String]
    val errBuf = ArrayBuffer.empty[String]
    val logger = ProcessLogger(line => outBuf += line, line => errBuf += line)
    val exit = Process(Seq(pythonExe, scriptPath.toString)).!(logger)

    val stdout = outBuf.mkString("\n")
    val stderr = errBuf.mkString("\n")
    if (exit != 0) {
      throw new StandaloneExecutionException(exit, scriptPath, source, stdout, stderr)
    }
    Result(outputPaths, stdout, stderr)
  }

  // Builds the full Python source: imports + prologue + verbatim operator body
  // + epilogue. We intentionally do NOT substitute the inNdf/outNdf placeholders
  // — the body keeps them so the var-bindings the prologue/epilogue introduce
  // (also named inNdf/outNdf) reference the same names.
  private def renderScript(
      body: String,
      inputs: Map[Int, Path],
      outputs: Map[Int, Path]
  ): String = {
    val sb = new StringBuilder

    sb.append("# Auto-generated by StandaloneRunner. Do not commit.\n")
    sb.append("import json\n")
    sb.append("import sys\n")
    sb.append("import pandas as pd\n")
    sb.append("import plotly.express as px\n")
    sb.append("import plotly.graph_objects as go\n")
    sb.append("import plotly.io\n")
    sb.append("\n")

    // Prologue: load each external input into in{N}df. Note: pd.read_json with
    // lines=True correctly handles empty files (returns empty DataFrame).
    inputs.toSeq.sortBy(_._1).foreach {
      case (n, path) =>
        sb.append(s"in${n}df = pd.read_json(${py(path.toString)}, lines=True)\n")
    }
    sb.append("\n")

    // Body verbatim — placeholders left in place.
    sb.append("# ── operator body ──\n")
    sb.append(body)
    if (!body.endsWith("\n")) sb.append('\n')
    sb.append("\n")

    // Epilogue: dump each out{N}df to JSONL. When producesDataFrame() is false
    // (visualization ops), `outputs` is empty and this block is a no-op — the
    // caller is expected to verify viz outputs by other means.
    outputs.toSeq.sortBy(_._1).foreach {
      case (n, path) =>
        sb.append(
          s"out${n}df.to_json(${py(path.toString)}, orient='records', lines=True)\n"
        )
    }

    sb.toString
  }

  // Python string literal, single-quoted with backslashes escaped. We
  // deliberately don't use repr() in Scala (no such thing) — JSON.toString
  // would also work but introduces double-quote escaping when the path has
  // spaces.
  private def py(s: String): String =
    "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

  // Resolution chain mirrors the rest of the Texera test infra: env var first
  // (set by CI / the shared-venv setup), then conventional names.
  private def resolvePython(): String = {
    val fromEnv = sys.env.get("UDF_PYTHON_PATH").filter(_.nonEmpty)
    fromEnv.getOrElse {
      // We don't try to probe `which` here — if neither env var nor a literal
      // `python3.12` is on PATH, the subprocess invocation will fail and the
      // error path below surfaces it.
      "python3.12"
    }
  }
}

final class StandaloneExecutionException(
    val exitCode: Int,
    val scriptPath: Path,
    val source: String,
    val stdout: String,
    val stderr: String
) extends RuntimeException(
      // The script path goes first in the message so a failing CI log makes it
      // immediately obvious which file to open. stderr ends the message because
      // the Python traceback (if any) is the most actionable signal.
      s"""Standalone Python script exited with code $exitCode.
         |Script: $scriptPath
         |--- stdout ---
         |$stdout
         |--- stderr ---
         |$stderr""".stripMargin
    )