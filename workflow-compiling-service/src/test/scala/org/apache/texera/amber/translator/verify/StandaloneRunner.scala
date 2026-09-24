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
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
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
  * The operator's code is wrapped in a prologue that reads each input file into
  * an `inNdf` and an epilogue that writes each `outNdf` back out, with the
  * generated body verbatim between them. `N` is 1-based and counts the
  * operator's external ports in declaration order, the translator's own
  * convention, so the caller has to hand inputs over in that order.
  */
object StandaloneRunner extends LazyLogging {

  /** The value both paths seed numpy's global RNG with. Any fixed number does;
    * what matters is that the two agree, so it is declared once here and
    * referenced by name from py_op_driver's comment.
    */
  private[verify] val VerifySeed: Int = 20260811

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
      pythonExe: String = resolvePython(),
      exactIntegers: Boolean = false,
      outputSchemas: Map[PortIdentity, Schema] = Map.empty
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

    val inputSchemas = schemasOf(inputs)
    val source =
      renderScript(
        // The sidecar says what each column was DECLARED as, which the JSONL
        // cannot carry.
        gen.generateStandaloneCode(inputSchemas),
        inputs,
        outputPaths,
        gen.standaloneHelpers(),
        gen.standaloneImports(),
        exactIntegers,
        integralOutputColumns(outputSchemas, outputPaths.keys.toSeq),
        gen.standaloneSourceName()
      )
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
      outputs: Map[Int, Path],
      helpers: Seq[String],
      imports: Seq[String],
      exactIntegers: Boolean,
      integralOutputs: Map[Int, Seq[String]],
      sourceFileName: Option[String]
  ): String = {
    val sb = new StringBuilder

    sb.append("# Auto-generated by StandaloneRunner. Do not commit.\n")
    sb.append("import json\n")
    sb.append("import sys\n")
    sb.append("import base64\n")
    sb.append("import pickle\n")
    // NOTE: nothing beyond pandas is injected here. The production translator
    // (WorkflowToPythonTranslator) emits pandas for every script and then only
    // what the operators in the plan ask for, so an operator whose standalone
    // code needs numpy, or plotly, must say so. Injecting either here would mask
    // that class of bug: the script would run in verify and fail on export.
    sb.append("import pandas as pd\n")
    imports.foreach(line => sb.append(line).append("\n"))
    // Same seed as py_op_driver's run_config, for the reason given there. Bound
    // under a private name and deleted so the note above still holds: a script
    // that wants numpy has to import it, and this does not hand it one.
    sb.append(s"import numpy as _texera_np; _texera_np.random.seed($VerifySeed); del _texera_np\n")
    sb.append("\n")

    // Object columns holding non-primitive values (e.g. a trained sklearn model
    // in a BINARY output column) can't go through to_json. Pickle+base64 them so
    // the JSONL matches py_op_driver's BINARY write path exactly. Primitives
    // (str/int/float/bool/None) pass through unchanged, so ordinary DataFrame
    // outputs are unaffected.
    sb.append("def _texera_encode_obj_cols(df):\n")
    // A numpy scalar is unwrapped rather than pickled, and pd.NA is written as
    // null. An operator that rebuilds rows out of an integer column hands back
    // neither a Python scalar nor None, so the branch below would put a base64
    // pickle into a column the schema declares a number.
    sb.append("    def _texera_plain(_v):\n")
    sb.append("        if _v is pd.NA:\n")
    sb.append("            return None\n")
    sb.append("        if hasattr(_v, 'item') and getattr(_v, 'ndim', None) == 0:\n")
    sb.append("            return _v.item()\n")
    sb.append("        return _v\n")
    sb.append("    for _c in df.columns:\n")
    sb.append("        if df[_c].dtype == object:\n")
    sb.append("            df[_c] = df[_c].map(_texera_plain)\n")
    // Bytes are written as the base64 the engine's writer writes, not as a
    // base64 pickle of them: the prologue hands a BINARY column over as bytes,
    // the way the engine hands one to an operator, and the branch below would
    // otherwise answer a column the run wrote as `aGk=` with a pickle of it.
    sb.append(
      "            df[_c] = df[_c].map(lambda _v: base64.b64encode(_v).decode('ascii') " +
        "if isinstance(_v, (bytes, bytearray)) else _v)\n"
    )
    sb.append(
      "            df[_c] = df[_c].map(lambda _v: base64.b64encode(pickle.dumps(_v)).decode('ascii') " +
        "if not isinstance(_v, (str, int, float, bool, type(None))) else _v)\n"
    )
    sb.append("    return df\n")
    sb.append("\n")

    // Both paths have to be handed the same numbers. `read_json` parses a column
    // holding a null through float64, so a LONG of 9007199254740993 arrives as
    // 9007199254740992 while the engine still has the tuple. Python's json reads
    // it exactly. Only for a JVM Path A: a Python operator's own table goes
    // through pandas too, so there the float is what BOTH sides see.
    if (exactIntegers) {
      sb.append("def _texera_exact_ints(_path, _columns):\n")
      sb.append("    _values = {_c: [] for _c in _columns}\n")
      sb.append("    with open(_path, 'r', encoding='utf-8') as _f:\n")
      sb.append("        for _line in _f:\n")
      sb.append("            _line = _line.strip()\n")
      sb.append("            if not _line:\n")
      sb.append("                continue\n")
      sb.append("            _row = json.loads(_line)\n")
      sb.append("            for _c in _columns:\n")
      sb.append("                _v = _row.get(_c)\n")
      sb.append("                _values[_c].append(pd.NA if _v is None else int(_v))\n")
      sb.append("    return {_c: pd.array(_v, dtype='Int64') for _c, _v in _values.items()}\n")
      sb.append("\n")
    }

    // TIMESTAMP columns are handed to the operator as datetime64 (see the
    // prologue below) to match the schema-typed runtime path, but the runtime
    // path serializes a TIMESTAMP back out with java.sql.Timestamp.toString —
    // "yyyy-mm-dd hh:mm:ss.f", trailing zeros trimmed to at least one digit —
    // whereas pandas' to_json would emit epoch millis. Convert datetime columns
    // back to that exact form before writing so both paths' JSONL agree.
    //
    // The fraction is built from the nanoseconds and not from %f, which stops at
    // six digits, while Timestamp.toString writes all nine.
    sb.append("def _texera_ts_str(_v):\n")
    sb.append("    if pd.isna(_v):\n")
    sb.append("        return None\n")
    sb.append("    _f = f'{_v.microsecond * 1000 + _v.nanosecond:09d}'.rstrip('0') or '0'\n")
    sb.append("    return _v.strftime('%Y-%m-%d %H:%M:%S.') + _f\n")
    sb.append("\n")
    sb.append("def _texera_encode_ts_cols(df):\n")
    sb.append("    for _c in df.columns:\n")
    sb.append("        if pd.api.types.is_datetime64_any_dtype(df[_c]):\n")
    sb.append("            df[_c] = df[_c].map(_texera_ts_str)\n")
    sb.append("    return df\n")
    sb.append("\n")

    // A TIMESTAMP is a java.sql.Timestamp on the engine side, which holds the
    // year 2500 as readily as 2024. pandas' default nanoseconds reach only 1677
    // to 2262, so a column outside that window is read at microseconds instead
    // of failing before the operator runs. A column inside it keeps the
    // nanoseconds, which Timestamp counts too.
    //
    // Cell by cell through dateutil and not astype: astype takes its unit from
    // the text, so a fraction of nine digits is parsed as nanoseconds again and
    // fails the same way. A datetime holds microseconds, so the rest is dropped.
    sb.append("def _texera_read_ts(_s):\n")
    sb.append("    try:\n")
    sb.append("        return pd.to_datetime(_s)\n")
    sb.append("    except pd.errors.OutOfBoundsDatetime:\n")
    sb.append("        from dateutil.parser import parse as _parse_date\n")
    sb.append("        _read = _s.map(lambda _v: None if pd.isna(_v) else _parse_date(_v))\n")
    sb.append("        return _read.astype('datetime64[us]')\n")
    sb.append("\n")

    // The engine writes a tuple through the schema, so a column it declares
    // INTEGER leaves as an integer however the operator held it. pandas has no
    // plain integer that carries a null, so the same column leaves the script
    // as 6.0 whenever a row is missing, and the two sides then disagree on
    // every value in it. The declared columns are written the way the engine
    // writes them: as integers, with the hole kept.
    //
    // Only where the values ARE whole. A column the script filled with 6.5
    // where the run had 6 is a real disagreement, and rounding it here would
    // report the two sides as equal.
    if (integralOutputs.values.exists(_.nonEmpty)) {
      sb.append("def _texera_int_cols(df, _columns):\n")
      // An operator that rebuilds its rows hands back a column of objects
      // rather than a float column, so the values are read one at a time: a
      // numpy scalar unwrapped, anything that is not a plain number left for
      // the branch below to refuse.
      sb.append("    def _texera_num(_v):\n")
      sb.append("        if hasattr(_v, 'item') and getattr(_v, 'ndim', None) == 0:\n")
      sb.append("            _v = _v.item()\n")
      sb.append("        if isinstance(_v, bool) or not isinstance(_v, (int, float)):\n")
      sb.append("            return None\n")
      sb.append("        return _v\n")
      sb.append("    for _c in _columns:\n")
      sb.append("        if _c not in df.columns or pd.api.types.is_integer_dtype(df[_c]):\n")
      sb.append("            continue\n")
      sb.append("        _known = [_texera_num(_v) for _v in df[_c] if not pd.isna(_v)]\n")
      sb.append("        if any(_v is None or float(_v) % 1 != 0 for _v in _known):\n")
      sb.append("            continue\n")
      sb.append("        df[_c] = pd.array(\n")
      sb.append("            [pd.NA if pd.isna(_v) else int(_v) for _v in df[_c]], dtype='Int64'\n")
      sb.append("        )\n")
      sb.append("    return df\n")
      sb.append("\n")
    }

    // Prologue: load each external input into in{N}df. Every option below undoes
    // an inference that would otherwise hand the two paths different data.
    //
    // convert_dates=False: read_json reads ISO-ish strings, and columns merely
    // NAMED like dates, as datetime64, so a plain date string would serialize
    // with a "T00:00:00" on one side only. It leaves real TIMESTAMP columns as
    // strings too, which the sidecar names and the casts below restore.
    //
    // precise_float=True: the default ujson parser is lossy in the last few
    // ULPs, and an operator that renders a DOUBLE as text prints the difference.
    //
    // dtype=object on the sidecar's STRING columns: "001" would arrive as 1, and
    // a column holding only nulls as NaN rather than None.
    inputs.toSeq.sortBy(_._1).foreach {
      case (n, path) =>
        val dtype = stringColumns(path) match {
          case Seq() => ""
          case cols  => cols.map(c => s"${py(c)}: 'object'").mkString(", dtype={", ", ", "}")
        }
        sb.append(
          s"in${n}df = pd.read_json(${py(path.toString)}, lines=True, " +
            s"convert_dates=False, precise_float=True$dtype)\n"
        )
        // read_json finds no column names in a file with no rows, so it produces a
        // frame of no columns. In an exported script an empty frame keeps its
        // columns, since a filter that matches nothing and a header-only CSV both
        // leave them in place. Rebuild them from the sidecar so the script is
        // handed the table it would get in a real run.
        emptyFrameColumns(path) match {
          case Seq() => ()
          case cols =>
            val fields = cols.map { case (c, d) => s"${py(c)}: pd.Series(dtype=${py(d)})" }
            sb.append(s"if in${n}df.empty:\n")
            sb.append(s"    in${n}df = pd.DataFrame({${fields.mkString(", ")}})\n")
        }
        timestampColumns(path).foreach { col =>
          sb.append(s"if ${py(col)} in in${n}df.columns:\n")
          sb.append(s"    in${n}df[${py(col)}] = _texera_read_ts(in${n}df[${py(col)}])\n")
        }
        doubleColumns(path).foreach { col =>
          sb.append(s"if ${py(col)} in in${n}df.columns:\n")
          sb.append(s"    in${n}df[${py(col)}] = in${n}df[${py(col)}].astype('float64')\n")
        }
        // A boolean column the fixture punched a hole in comes back as float64,
        // so the operator is handed 1.0 and 0.0 where the run had true and
        // false. The nullable dtype carries both the values and the hole. One
        // without a hole already arrives as bool and is left alone, the way an
        // exact integer is: the nullable dtype would be one the run never had.
        booleanColumns(path).foreach { col =>
          sb.append(
            s"if ${py(col)} in in${n}df.columns and not pd.api.types.is_bool_dtype(" +
              s"in${n}df[${py(col)}]):\n"
          )
          sb.append(s"    in${n}df[${py(col)}] = in${n}df[${py(col)}].astype('boolean')\n")
        }
        // The fixture carries a BINARY column as base64 text, which is how JSON
        // carries bytes at all, and the engine decodes it before the operator
        // sees it (TupleIO.readTuples). Left as text, the script's operator is
        // handed a str where the run's was handed bytes, and anything it does
        // with them -- a length, a decode, a digest -- answers for the base64
        // rather than for the value.
        binaryColumns(path).foreach { col =>
          sb.append(s"if ${py(col)} in in${n}df.columns:\n")
          sb.append(
            s"    in${n}df[${py(col)}] = in${n}df[${py(col)}].map(\n" +
              s"        lambda _v: None if pd.isna(_v) else base64.b64decode(_v)\n" +
              s"    )\n"
          )
        }
        // Only where the reader lost the value: a column with no holes already
        // came back exact, and replacing it would hand the operator a nullable
        // dtype the run never had.
        if (exactIntegers) integerColumns(path) match {
          case Seq() => ()
          case cols =>
            val names = cols.map(py).mkString(", ")
            sb.append(
              s"for _c, _v in _texera_exact_ints(${py(path.toString)}, [$names]).items():\n"
            )
            sb.append(s"    if _c in in${n}df.columns and not pd.api.types.is_integer_dtype(")
            sb.append(s"in${n}df[_c]):\n")
            sb.append(s"        in${n}df[_c] = _v\n")
        }
    }
    // The variadic placeholder, bound here for the same reason the numbered ones
    // are: this script leaves the body's placeholders alone and defines names to
    // match them, so an operator reading a variadic port finds its list here the
    // way the translator would have written one out.
    if (inputs.nonEmpty) {
      sb.append(
        inputs.keys.toSeq.sorted.map(n => s"in${n}df").mkString("inAlldf = [", ", ", "]\n")
      )
    }
    // The file placeholders, bound the same way. A script this runner builds
    // holds one operator, so the plain names are unambiguous and the comparison
    // knows where to look.
    sb.append("outputHtml = \"output.html\"\n")
    sb.append("outputJson = \"output.json\"\n")
    sb.append("\n")

    // Body verbatim — placeholders left in place.
    // Emitted ahead of the body the way the translator does, so an operator that
    // declares a helper is exercised here exactly as it runs in a real script.
    helpers.foreach { helper =>
      sb.append(helper)
      if (!helper.endsWith("\n")) sb.append('\n')
      sb.append('\n')
    }

    // A source names the file it reads by placeholder and leaves the naming to
    // whoever assembles the script, the translator doing it across a whole plan
    // so that two sources reading different files whose paths end alike do not
    // both ask for the same one. This runner assembles a single operator, so
    // there is nobody to collide with and the name the source offers stands.
    // Without this the body reads a bare `sourceFile` that was never bound.
    sourceFileName.foreach { name =>
      sb.append(s"${StandaloneCodeGenerator.SourceFilePlaceholder} = ${py(name)}\n\n")
    }

    sb.append("# ── operator body ──\n")
    sb.append(body)
    if (!body.endsWith("\n")) sb.append('\n')
    sb.append("\n")

    // Epilogue: dump each out{N}df to JSONL. When producesDataFrame() is false
    // (visualization ops), `outputs` is empty and this block is a no-op — the
    // caller is expected to verify viz outputs by other means.
    outputs.toSeq.sortBy(_._1).foreach {
      case (n, path) =>
        val frame = integralOutputs.getOrElse(n, Seq.empty) match {
          case Seq() => s"out${n}df"
          case cols  => s"_texera_int_cols(out${n}df, [${cols.map(py).mkString(", ")}])"
        }
        sb.append(
          s"_texera_encode_obj_cols(_texera_encode_ts_cols($frame))" +
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

  // STRING-typed column names, for the read_json dtype map above.
  /** The declared schema behind each input port, read from the sidecars. A port the
    * sidecar does not cover is left out rather than guessed at.
    */
  private def schemasOf(inputs: Map[Int, Path]): Map[PortIdentity, Schema] =
    inputs.flatMap {
      case (port, path) =>
        scala.util
          .Try(TupleIO.readSchemaSidecar(path))
          .toOption
          .map(schema => PortIdentity(port - 1) -> schema)
    }

  /** Each declared column with the pandas dtype it holds when it has values in it.
    * Only the types a fixture can hold are named; anything else falls back to
    * object, the dtype an inferred column of unknown content would have had anyway.
    */
  private def emptyFrameColumns(input: Path): Seq[(String, String)] =
    scala.util
      .Try(TupleIO.readSchemaSidecar(input))
      .toOption
      .toSeq
      .flatMap(_.getAttributes.map { attr =>
        val dtype = attr.getType match {
          case AttributeType.INTEGER   => "int32"
          case AttributeType.LONG      => "int64"
          case AttributeType.DOUBLE    => "float64"
          case AttributeType.BOOLEAN   => "bool"
          case AttributeType.TIMESTAMP => "datetime64[us]"
          case _                       => "object"
        }
        attr.getName -> dtype
      })

  /** The columns the sidecar declares integral, both widths: the loss is the same
    * for either.
    */
  private def integerColumns(input: Path): Seq[String] =
    columnsOfType(input, AttributeType.INTEGER) ++ columnsOfType(input, AttributeType.LONG)

  private def stringColumns(input: Path): Seq[String] =
    columnsOfType(input, AttributeType.STRING)

  /** BOOLEAN-typed column names. A hole makes read_json read the whole column
    * as float64, so the run's true and false reach the operator as 1.0 and 0.0.
    */
  private def booleanColumns(input: Path): Seq[String] =
    columnsOfType(input, AttributeType.BOOLEAN)

  /** BINARY-typed column names. JSON carries bytes as base64 text, and the
    * engine decodes it before the operator sees the field.
    */
  private def binaryColumns(input: Path): Seq[String] =
    columnsOfType(input, AttributeType.BINARY)

  /** Per output port, the columns the schema declares integral, which is what
    * the engine's writer goes by.
    *
    * The schemas are the ones the run itself produced rather than a propagation
    * of this operator's own: asking an operator to propagate a second time
    * builds it a second physical plan, and Aggregate's two-stage one does not
    * survive that. A caller with no schemas to hand over names no column, and
    * the epilogue then writes each frame as pandas holds it.
    */
  private def integralOutputColumns(
      outputSchemas: Map[PortIdentity, Schema],
      ports: Seq[Int]
  ): Map[Int, Seq[String]] =
    ports.map { port =>
      val declared = outputSchemas
        .get(PortIdentity(port - 1))
        .toSeq
        .flatMap(_.getAttributes)
        .filter(a => a.getType == AttributeType.INTEGER || a.getType == AttributeType.LONG)
        .map(_.getName)
      port -> declared
    }.toMap

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
  // spaces. Control characters are escaped too: a fixture column name can hold
  // a literal newline, which raw would end the literal and break the script.
  private def py(s: String): String =
    s.map {
      case '\\'                => "\\\\"
      case '\''                => "\\'"
      case c if c.toInt < 0x20 => f"\\x${c.toInt}%02x"
      case c                   => c.toString
    }.mkString("'", "", "'")

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
