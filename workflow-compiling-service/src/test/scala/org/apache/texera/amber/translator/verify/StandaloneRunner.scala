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
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.util.python.PythonWorkerPool

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

    val (exit, stdout, stderr) = execute(scriptPath, workDir, pythonExe)
    if (exit != 0) {
      throw new StandaloneExecutionException(exit, scriptPath, source, stdout, stderr)
    }
    Result(outputPaths, stdout, stderr)
  }

  private val WorkerResourcePath = "/python/standalone_worker.py"

  // Run the rendered script and return (exitCode, stdout, stderr). Prefers a
  // pooled persistent worker (imports pandas/plotly once, ~18x faster per op —
  // see PythonWorkerPool); a rare hard worker crash falls back to a one-shot
  // subprocess so behavior is never worse than the original path. Both paths
  // run with cwd = workDir and read results from files, so they are
  // interchangeable — the executed script is byte-identical.
  private def execute(scriptPath: Path, workDir: Path, pythonExe: String): (Int, String, String) = {
    if (PythonWorkerPool.enabled) {
      try {
        val req = org.apache.texera.amber.util.JSONUtils.objectMapper.createObjectNode()
        req.put("scriptPath", scriptPath.toString)
        req.put("workDir", workDir.toString)
        val o = PythonWorkerPool.run(WorkerResourcePath, Seq.empty, pythonExe, req)
        return (o.exit, o.stdout, o.stderr)
      } catch {
        case e: PythonWorkerPool.WorkerDiedException =>
          logger.warn(
            s"Standalone worker unavailable; falling back to one-shot subprocess " +
              s"for $scriptPath: ${e.getMessage}"
          )
      }
    }
    runSubprocess(scriptPath, workDir, pythonExe)
  }

  // Original one-process-per-operator path. Retained as the fallback and as the
  // behavior selected by TEXERA_TEST_PYTHON_WORKER=0.
  private def runSubprocess(
      scriptPath: Path,
      workDir: Path,
      pythonExe: String
  ): (Int, String, String) = {
    // Capture stdout/stderr separately. ProcessLogger's append is called from
    // the subprocess's I/O thread, so we collect into ArrayBuffer (thread-safe
    // append is fine for this serial use) and join at the end.
    val outBuf = ArrayBuffer.empty[String]
    val errBuf = ArrayBuffer.empty[String]
    val logger = ProcessLogger(line => outBuf += line, line => errBuf += line)
    // cwd = workDir so generated code using *relative* paths (e.g. CSVScan's
    // basename-stripped `pd.read_csv("sample.csv")`) resolves against workDir.
    // Absolute paths written by the prologue/epilogue are unaffected.
    val exit = Process(Seq(pythonExe, scriptPath.toString), Some(workDir.toFile)).!(logger)
    (exit, outBuf.mkString("\n"), errBuf.mkString("\n"))
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
    sb.append("import base64\n")
    sb.append("import pickle\n")
    // NOTE: numpy is intentionally NOT injected here. The production translator
    // (WorkflowToPythonTranslator) only provides pandas + plotly to standalone
    // scripts, so any operator whose standalone code needs numpy must import it
    // itself. Injecting numpy here would mask that class of bug in verify tests.
    sb.append("import pandas as pd\n")
    sb.append("import plotly.express as px\n")
    sb.append("import plotly.graph_objects as go\n")
    sb.append("import plotly.io\n")
    sb.append("\n")

    // Object columns holding non-primitive values (e.g. a trained sklearn model
    // in a BINARY output column) can't go through to_json. Pickle+base64 them so
    // the JSONL matches py_op_driver's BINARY write path exactly. Primitives
    // (str/int/float/bool/None) pass through unchanged, so ordinary DataFrame
    // outputs are unaffected.
    sb.append("def _texera_encode_obj_cols(df):\n")
    sb.append("    for _c in df.columns:\n")
    sb.append("        if df[_c].dtype == object:\n")
    sb.append(
      "            df[_c] = df[_c].map(lambda _v: base64.b64encode(pickle.dumps(_v)).decode('ascii') " +
        "if not isinstance(_v, (str, int, float, bool, type(None))) else _v)\n"
    )
    sb.append("    return df\n")
    sb.append("\n")

    // TIMESTAMP columns are handed to the operator as datetime64 (see the
    // prologue below) to match the schema-typed runtime path, but the runtime
    // path serializes a TIMESTAMP back out with java.sql.Timestamp.toString —
    // "yyyy-mm-dd hh:mm:ss.f", trailing zeros trimmed to at least one digit —
    // whereas pandas' to_json would emit epoch millis. Convert datetime columns
    // back to that exact form before writing so both paths' JSONL agree.
    sb.append("def _texera_ts_str(_v):\n")
    sb.append("    if pd.isna(_v):\n")
    sb.append("        return None\n")
    sb.append("    _s = _v.strftime('%Y-%m-%d %H:%M:%S.%f').rstrip('0')\n")
    sb.append("    return _s + '0' if _s.endswith('.') else _s\n")
    sb.append("\n")
    sb.append("def _texera_encode_ts_cols(df):\n")
    sb.append("    for _c in df.columns:\n")
    sb.append("        if pd.api.types.is_datetime64_any_dtype(df[_c]):\n")
    sb.append("            df[_c] = df[_c].map(_texera_ts_str)\n")
    sb.append("    return df\n")
    sb.append("\n")

    // Prologue: load each external input into in{N}df. Note: pd.read_json with
    // lines=True correctly handles empty files (returns empty DataFrame).
    // convert_dates=False: pd.read_json otherwise auto-coerces ISO-ish strings
    // and columns named like dates ("date", "*_at", …) to datetime64, which the
    // schema-typed runtime path (STRING) does not do — that divergence would
    // make a plain date string column serialize as "...T00:00:00" on only one
    // side. Operators that genuinely need datetimes convert explicitly, so both
    // paths stay in sync.
    // precise_float=True: pd.read_json's default (ujson) fast double parser is
    // lossy in the last few ULPs, so a DOUBLE column would load slightly
    // different values than the schema-typed runtime path (which parses doubles
    // exactly). Operators that stringify raw cell values (e.g. Radar hover text)
    // then diverge; precise_float=True keeps both paths bit-identical.
    // The blanket convert_dates=False also leaves genuine TIMESTAMP columns as
    // strings, which the runtime path delivers as datetime64 — a divergence for
    // any operator that renders or computes on them. The fixture's schema
    // sidecar says which columns those are, so cast exactly those back.
    inputs.toSeq.sortBy(_._1).foreach {
      case (n, path) =>
        sb.append(
          s"in${n}df = pd.read_json(${py(path.toString)}, lines=True, convert_dates=False, precise_float=True)\n"
        )
        timestampColumns(path).foreach { col =>
          sb.append(s"if ${py(col)} in in${n}df.columns:\n")
          sb.append(s"    in${n}df[${py(col)}] = pd.to_datetime(in${n}df[${py(col)}])\n")
        }
        doubleColumns(path).foreach { col =>
          sb.append(s"if ${py(col)} in in${n}df.columns:\n")
          sb.append(s"    in${n}df[${py(col)}] = in${n}df[${py(col)}].astype('float64')\n")
        }
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
          s"_texera_encode_obj_cols(_texera_encode_ts_cols(out${n}df))" +
            s".to_json(${py(path.toString)}, orient='records', lines=True)\n"
        )
    }

    sb.toString
  }

  // TIMESTAMP-typed column names from a fixture's `.jsonl.schema.json` sidecar.
  // A missing or unreadable sidecar means no casts — the prologue then behaves
  // exactly as before.
  private def timestampColumns(input: Path): Seq[String] =
    columnsOfType(input, AttributeType.TIMESTAMP)

  // DOUBLE-typed column names. pd.read_json narrows a float column whose values
  // are all integral to int64, while the runtime path keeps the schema's DOUBLE,
  // so a column like 7.0 stringifies as "7" on one side and "7.0" on the other —
  // invisible to numeric comparison, visible the moment an operator uses the
  // column as a label (a trace name, a legend entry, hover text).
  private def doubleColumns(input: Path): Seq[String] =
    columnsOfType(input, AttributeType.DOUBLE)

  private def columnsOfType(input: Path, attributeType: AttributeType): Seq[String] =
    scala.util
      .Try(TupleIO.readSchemaSidecar(input))
      .toOption
      .toSeq
      .flatMap(
        _.getAttributes.filter(_.getType == attributeType).map(_.getName)
      )

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
