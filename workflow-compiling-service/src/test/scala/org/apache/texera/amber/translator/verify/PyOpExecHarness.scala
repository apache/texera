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

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalPlan, PortIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.python.PythonWorkerPool

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

/**
  * Counterpart to [[OpExecHarness]] for Python-native operators
  * ([[OpExecWithCode]] with language="python"). Drives the operator's
  * generatePythonCode() output through a thin subprocess driver rather than
  * spinning up the Pekko/Arrow worker stack.
  *
  * Same Result(outputs, outputSchemas) shape as OpExecHarness so the rest of
  * the verify pipeline (Comparator, category runners) is harness-agnostic.
  *
  * Scope (MVP, mirrors OpExecHarness's MVP):
  *   - Single-PhysicalOp plans only. PythonOperatorDescriptor only emits
  *     either a `sourcePhysicalOp` or a `oneToOnePhysicalOp`, so multi-op
  *     plans don't exist for Python-native ops today. If that changes, add
  *     topo-order driving here the way OpExecHarness does.
  *   - Single output port. UDFOperatorV2 / UDFTableOperator / UDFBatchOperator
  *     / UDFSourceOperator all yield TupleLike without specifying a port —
  *     same convention OpExecHarness uses when port is unset.
  *   - JSONL types: STRING / INTEGER / LONG / DOUBLE / BOOLEAN. TIMESTAMP /
  *     BINARY / LARGE_BINARY require explicit codecs in both [[TupleIO]] and
  *     the driver — add when the first operator needs them.
  */
object PyOpExecHarness extends LazyLogging {

  private val TestWorkflowId = WorkflowIdentity(0L)
  private val TestExecutionId = ExecutionIdentity(0L)

  // Same Result shape as OpExecHarness so callers can swap harnesses
  // transparently.
  final case class Result(
      outputs: Map[PortIdentity, Path],
      outputSchemas: Map[PortIdentity, Schema]
  )

  // Driver script lives on the test classpath at /python/py_op_driver.py
  // (sibling to compare.py). Extracted to a temp file at runtime so it works
  // whether the test resources are loose files or sealed in a jar.
  private val DriverResourcePath = "/python/py_op_driver.py"

  def execute(
      opDesc: LogicalOp,
      inputs: Map[PortIdentity, Path],
      outputDir: Path,
      pythonExe: String = resolvePython(),
      amberPythonHome: Path = resolveAmberPythonHome()
  ): Result = {
    Files.createDirectories(outputDir)

    val plan = opDesc.getPhysicalPlan(TestWorkflowId, TestExecutionId)

    // PythonOperatorDescriptor builds single-op plans; bail loudly if some
    // future Python op produces a multi-stage plan (need to extend the driver
    // and the per-PhysicalOp config the way OpExecHarness does).
    require(
      plan.operators.size == 1,
      s"PyOpExecHarness only supports single-PhysicalOp plans for now, got " +
        s"${plan.operators.size} PhysicalOps"
    )
    val phOp = plan.operators.head

    val (pythonCode, language) = phOp.opExecInitInfo match {
      case OpExecWithCode(code, lang) => (code, lang)
      case other =>
        throw new UnsupportedOperationException(
          s"PyOpExecHarness only supports OpExecWithCode; got ${other.getClass.getSimpleName}. " +
            "For OpExecWithClassName, use OpExecHarness."
        )
    }
    require(
      language == "python",
      s"""PyOpExecHarness only supports language="python", got "$language"."""
    )

    // External input ports = same definition as OpExecHarness. For a
    // single-op plan that's just every input port the op declares.
    val externalInputs: Set[(PhysicalOpIdentity, PortIdentity)] =
      phOp.inputPorts.keys.map(portId => (phOp.id, portId)).toSet
    OpExecHarness.validateInputCoverage(externalInputs, inputs.keySet)

    val inputSchemas: Map[PortIdentity, Schema] =
      inputs.map { case (portId, path) => portId -> TupleIO.readSchemaSidecar(path) }

    // Preparing the plan is the same work for either executor, so it is done in
    // one place; only what runs the prepared op differs between the harnesses.
    val planWithSchemas =
      OpExecHarness.propagateExternalSchemas(plan, externalInputs, inputSchemas)
    val phOpWithSchemas = planWithSchemas.operators.head

    // Output port schemas come from PhysicalPlan.propagateSchema — same
    // ground truth OpExecHarness writes to its own outputs.
    val outputPortSchemas: Map[PortIdentity, Schema] =
      phOpWithSchemas.outputPorts.map {
        case (portId, (_, _, schemaOrErr)) =>
          portId -> schemaOrErr.toOption.getOrElse(
            throw new IllegalStateException(
              s"Output schema for ($portId) was not propagated"
            )
          )
      }

    require(
      outputPortSchemas.size == 1,
      s"PyOpExecHarness only supports single-output-port operators, got " +
        s"${outputPortSchemas.size} output ports"
    )
    val (outputPortId, outputSchema) = outputPortSchemas.head
    val outputPath = outputDir.resolve(s"output_port_${outputPortId.id}.jsonl")

    // Port ordering for multi-input ops: respect declared dependencies
    // (matches OpExecHarness — e.g. HashJoin probe processes build-side
    // first). Default = sorted by port id when no dependencies declared.
    val portOrder: Seq[Int] =
      if (phOpWithSchemas.getInputPortDependencyPairs.nonEmpty)
        phOpWithSchemas.getInputPortDependencyPairs.map(_.id)
      else phOpWithSchemas.inputPorts.keys.toList.map(_.id).sorted

    val config = buildConfig(
      pythonCode = pythonCode,
      isSource = phOpWithSchemas.isSourceOperator,
      portOrder = portOrder,
      inputs = inputs,
      outputPath = outputPath,
      outputSchema = outputSchema
    )

    val configPath = outputDir.resolve("py_op_driver_config.json")
    Files.write(configPath, config.getBytes(StandardCharsets.UTF_8))

    val driverPath = extractDriverScript()
    runDriver(driverPath, configPath, outputDir, pythonExe, amberPythonHome)

    Result(
      outputs = Map(outputPortId -> outputPath),
      outputSchemas = Map(outputPortId -> outputSchema)
    )
  }

  // --------------------------------------------------------------------------
  // Config serialization. Matches the driver's expected schema (see
  // py_op_driver.py's module docstring).
  // --------------------------------------------------------------------------
  private def buildConfig(
      pythonCode: String,
      isSource: Boolean,
      portOrder: Seq[Int],
      inputs: Map[PortIdentity, Path],
      outputPath: Path,
      outputSchema: Schema
  ): String = {
    val root: ObjectNode = objectMapper.createObjectNode()
    root.put("operatorCode", pythonCode)
    root.put("isSource", isSource)

    val portOrderArr: ArrayNode = root.putArray("portOrder")
    portOrder.foreach(portOrderArr.add)

    val inputsArr: ArrayNode = root.putArray("inputs")
    inputs.toSeq.sortBy(_._1.id).foreach {
      case (portId, dataPath) =>
        val entry: ObjectNode = inputsArr.addObject()
        entry.put("portIndex", portId.id)
        entry.put("dataPath", dataPath.toAbsolutePath.toString)
      // schemaPath is implicit (data_path + ".schema.json") — the driver
      // resolves it the same way TupleIO does.
    }

    val outputsArr: ArrayNode = root.putArray("outputs")
    val outEntry: ObjectNode = outputsArr.addObject()
    outEntry.put("dataPath", outputPath.toAbsolutePath.toString)
    // Embed the schema directly. We can't just write the sidecar ahead of
    // time and have the driver read it, because writing a sidecar before
    // outputs exist would leave a stale sidecar on partial failures.
    outEntry.set[ObjectNode](
      "schema",
      objectMapper.valueToTree[ObjectNode](outputSchema)
    )

    objectMapper.writeValueAsString(root)
  }

  // --------------------------------------------------------------------------
  // Subprocess invocation.
  // --------------------------------------------------------------------------
  private def runDriver(
      driverPath: Path,
      configPath: Path,
      cwd: Path,
      pythonExe: String,
      amberPythonHome: Path
  ): Unit = {
    // Prepend amber's Python source to PYTHONPATH so `import pytexera`
    // resolves. Existing PYTHONPATH (if any) is preserved as the lower-
    // priority suffix.
    val existing = sys.env.getOrElse("PYTHONPATH", "")
    val newPyPath =
      if (existing.isEmpty) amberPythonHome.toAbsolutePath.toString
      else s"${amberPythonHome.toAbsolutePath}${File.pathSeparator}$existing"

    val (exit, stdout, stderr) = execDriver(driverPath, configPath, cwd, pythonExe, newPyPath)
    if (exit != 0) {
      throw new PyOpDriverException(
        exitCode = exit,
        driverPath = driverPath,
        configPath = configPath,
        stdout = stdout,
        stderr = stderr
      )
    }
  }

  // Prefer a pooled persistent worker (imports pytexera/pyamber once via
  // `py_op_driver.py --serve`, the ~300 ms cost that dominates a per-Python-op
  // run — see PythonWorkerPool). A rare hard worker crash falls back to a
  // one-shot subprocess so behavior is never worse than the original path. Both
  // paths use absolute config paths, so cwd only matters to the subprocess
  // form; the worker constructs a fresh operator per job for isolation.
  private def execDriver(
      driverPath: Path,
      configPath: Path,
      cwd: Path,
      pythonExe: String,
      pythonPath: String
  ): (Int, String, String) = {
    if (PythonWorkerPool.enabled) {
      try {
        val req = objectMapper.createObjectNode()
        req.put("configPath", configPath.toAbsolutePath.toString)
        val o = PythonWorkerPool.run(
          DriverResourcePath,
          Seq("--serve"),
          pythonExe,
          req,
          env = Map("PYTHONPATH" -> pythonPath)
        )
        return (o.exit, o.stdout, o.stderr)
      } catch {
        case e: PythonWorkerPool.WorkerDiedException =>
          logger.warn(
            s"py_op_driver worker unavailable; falling back to one-shot subprocess " +
              s"for $configPath: ${e.getMessage}"
          )
      }
    }
    runDriverSubprocess(driverPath, configPath, cwd, pythonExe, pythonPath)
  }

  // Original one-process-per-operator path. Retained as the fallback and as the
  // behavior selected by TEXERA_TEST_PYTHON_WORKER=0.
  private def runDriverSubprocess(
      driverPath: Path,
      configPath: Path,
      cwd: Path,
      pythonExe: String,
      pythonPath: String
  ): (Int, String, String) = {
    val outBuf = ArrayBuffer.empty[String]
    val errBuf = ArrayBuffer.empty[String]
    val procLogger = ProcessLogger(line => outBuf += line, line => errBuf += line)
    val exit = Process(
      Seq(pythonExe, driverPath.toString, configPath.toString),
      Some(cwd.toFile),
      "PYTHONPATH" -> pythonPath
    ).!(procLogger)
    (exit, outBuf.mkString("\n"), errBuf.mkString("\n"))
  }

  // --------------------------------------------------------------------------
  // Resolution helpers.
  // --------------------------------------------------------------------------
  private def resolvePython(): String =
    sys.env.get("UDF_PYTHON_PATH").filter(_.nonEmpty).getOrElse("python3.12")

  /**
    * Locate `amber/src/main/python`. Resolution chain:
    *   1. Env var TEXERA_AMBER_PYTHON_HOME (set by CI / dev shell).
    *   2. Walk up from cwd looking for `amber/src/main/python`.
    * sbt runs tests with cwd = the subproject dir (`workflow-compiling-service/`),
    * so the walk-up is two levels at most for the normal layout.
    */
  private def resolveAmberPythonHome(): Path = {
    sys.env.get("TEXERA_AMBER_PYTHON_HOME").filter(_.nonEmpty).map(Paths.get(_)).getOrElse {
      val cwd = Paths.get(".").toAbsolutePath.normalize()
      val maxDepth = 5
      var current: Path = cwd
      var depth = 0
      while (current != null && depth <= maxDepth) {
        val candidate = current.resolve("amber/src/main/python")
        if (Files.isDirectory(candidate)) return candidate.toAbsolutePath
        current = current.getParent
        depth += 1
      }
      throw new RuntimeException(
        s"PyOpExecHarness: could not locate amber/src/main/python from cwd $cwd. " +
          "Set TEXERA_AMBER_PYTHON_HOME to the absolute path."
      )
    }
  }

  private def extractDriverScript(): Path = {
    val stream = getClass.getResourceAsStream(DriverResourcePath)
    require(
      stream != null,
      s"py_op_driver.py not found on classpath at $DriverResourcePath"
    )
    try {
      val tmp = Files.createTempFile("py_op_driver-", ".py")
      Files.copy(stream, tmp, StandardCopyOption.REPLACE_EXISTING)
      tmp.toFile.deleteOnExit()
      tmp
    } finally stream.close()
  }

}

final class PyOpDriverException(
    val exitCode: Int,
    val driverPath: Path,
    val configPath: Path,
    val stdout: String,
    val stderr: String
) extends RuntimeException(
      s"""py_op_driver.py exited with code $exitCode.
         |Driver: $driverPath
         |Config: $configPath
         |--- stdout ---
         |$stdout
         |--- stderr ---
         |$stderr""".stripMargin
    )
