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

package org.apache.texera.amber.operator.source.scan.json

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class JSONLScanSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "JSONLScanSourceOpDesc.operatorInfo" should
    "advertise the JSONL file-scan name in the Data Input group with no input and one output" in {
    val info = (new JSONLScanSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "JSONL File Scan"
    info.operatorDescription shouldBe "Scan data from a JSONL file"
    info.operatorGroupName shouldBe OperatorGroupConstants.INPUT_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "JSONLScanSourceOpDesc" should "default the flatten flag, encoding, and scan window" in {
    val d = new JSONLScanSourceOpDesc
    d.flatten shouldBe false
    d.fileName shouldBe None
    d.fileEncoding shouldBe FileDecodingMethod.UTF_8
    d.limit shouldBe None
    d.offset shouldBe None
    d.fileTypeName shouldBe Some("JSONL")
  }

  "JSONLScanSourceOpDesc.sourceSchema" should "prompt for a file before one is resolved" in {
    val ex = intercept[IllegalArgumentException]((new JSONLScanSourceOpDesc).sourceSchema())
    ex.getMessage should include("No file selected")
  }

  "JSONLScanSourceOpDesc.getPhysicalOp" should
    "wire the JSONL exec as a source op with no input port and one output port" in {
    val d = new JSONLScanSourceOpDesc
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.scan.json.JSONLScanSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.parallelizable shouldBe true
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "JSONLScanSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new JSONLScanSourceOpDesc
    d.flatten = true
    d.fileEncoding = FileDecodingMethod.UTF_16
    d.limit = Some(10)
    d.offset = Some(5)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[JSONLScanSourceOpDesc]
    val r = restored.asInstanceOf[JSONLScanSourceOpDesc]
    r.flatten shouldBe true
    r.fileEncoding shouldBe FileDecodingMethod.UTF_16
    r.limit shouldBe Some(10)
    r.offset shouldBe Some(5)
  }

  // The executor drops and takes on the raw lines, so a line the window skips is
  // never read as JSON. Reading the file whole and slicing the frame afterwards
  // ends the export on a line the workflow never looked at.
  "JSONLScanSourceOpDesc.generateStandaloneCode" should
    "skip a line the window excludes without parsing it" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("jsonl-window-")
    dir.toFile.deleteOnExit()
    val data = dir.resolve("input.jsonl")
    Files.write(
      data,
      "not json at all\n{\"id\":1}\n{\"id\":2}\n".getBytes(StandardCharsets.UTF_8)
    )

    val op = new JSONLScanSourceOpDesc
    op.fileName = Some(data.toString)
    op.offset = Some(1)

    val script = dir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${op.generateStandaloneCode()}
         |print(list(out1df["id"]))
         |""".stripMargin.getBytes(StandardCharsets.UTF_8)
    )

    val process = new ProcessBuilder(python, script.toString)
      .directory(dir.toFile)
      .redirectErrorStream(true)
      .start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\n") {
      process.exitValue() shouldBe 0
      out.trim should endWith("[1, 2]")
    }
  }

  // Python resolution follows FilledAreaPlotOpDescSpec: udf.conf python.path
  // (UDF_PYTHON_PATH), then python3 / python / py.
  private def resolvePython(): Option[String] = {
    def fromConfig: Option[String] =
      Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
        .orElse(Try(ConfigFactory.load()).toOption)
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)

    def runnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
          else p.exitValue() == 0
        }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(runnable)
  }

  private def canImportPandas(python: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", "import pandas").redirectErrorStream(true).start()
    ).toOption
      .exists { p =>
        if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
        else p.exitValue() == 0
      }
}
