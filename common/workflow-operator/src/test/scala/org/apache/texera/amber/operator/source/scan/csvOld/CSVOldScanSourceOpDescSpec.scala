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

package org.apache.texera.amber.operator.source.scan.csvOld

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class CSVOldScanSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "CSVOldScanSourceOpDesc.operatorInfo" should
    "advertise the CSVOld file-scan name in the Data Input group with no input and one output" in {
    val info = (new CSVOldScanSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "CSVOld File Scan"
    info.operatorDescription shouldBe "Scan data from a CSVOld file"
    info.operatorGroupName shouldBe OperatorGroupConstants.INPUT_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "CSVOldScanSourceOpDesc" should "default the delimiter, header flag, and scan window" in {
    val d = new CSVOldScanSourceOpDesc
    d.customDelimiter shouldBe Some(",")
    d.hasHeader shouldBe true
    d.fileName shouldBe None
    d.fileEncoding shouldBe FileDecodingMethod.UTF_8
    d.limit shouldBe None
    d.offset shouldBe None
    d.fileTypeName shouldBe Some("CSVOld")
  }

  "CSVOldScanSourceOpDesc.sourceSchema" should "prompt for a file before one is resolved" in {
    val ex = intercept[IllegalArgumentException]((new CSVOldScanSourceOpDesc).sourceSchema())
    ex.getMessage should include("No file selected")
  }

  "CSVOldScanSourceOpDesc.getPhysicalOp" should
    "wire the CSVOld exec as a source op with no input port and one output port" in {
    val d = new CSVOldScanSourceOpDesc
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.scan.csvOld.CSVOldScanSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  it should "fall back to a comma when the configured delimiter is empty" in {
    val d = new CSVOldScanSourceOpDesc
    d.customDelimiter = Some("")
    d.getPhysicalOp(workflowId, executionId)
    d.customDelimiter shouldBe Some(",")
  }

  "CSVOldScanSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new CSVOldScanSourceOpDesc
    d.customDelimiter = Some(";")
    d.hasHeader = false
    d.fileEncoding = FileDecodingMethod.UTF_16
    d.limit = Some(10)
    d.offset = Some(5)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[CSVOldScanSourceOpDesc]
    val r = restored.asInstanceOf[CSVOldScanSourceOpDesc]
    r.customDelimiter shouldBe Some(";")
    r.hasHeader shouldBe false
    r.fileEncoding shouldBe FileDecodingMethod.UTF_16
    r.limit shouldBe Some(10)
    r.offset shouldBe Some(5)
  }

  // The limit bounded the sample the inference reads as well as the rows the
  // operator emits, so a Limit of 0 had nothing to infer from. The types came
  // back empty while the header still asked each column for one, and the
  // operator threw before a row was read. A file's columns do not depend on how
  // many of its rows were asked for.
  "CSVOldScanSourceOpDesc.sourceSchema" should "keep the file's columns when the window asks for no rows" in {
    val d = describing(writeCsv("id,name\n1,alice\n2,bob\n"))
    d.limit = Some(0)

    val schema = d.sourceSchema()
    schema.getAttributeNames shouldBe List("id", "name")
    schema.getAttribute("id").getType shouldBe AttributeType.INTEGER
    schema.getAttribute("name").getType shouldBe AttributeType.STRING
  }

  // With no header line left after the offset, pandas had nothing to count the
  // columns from and the script raised where the engine emits no rows.
  "CSVOldScanSourceOpDesc.generateStandaloneCode" should
    "read no rows with the schema's columns, as the engine does, when a headerless offset passes the end" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val path = writeCsv("1,a\n2,b\n")
    val d = describing(path)
    d.hasHeader = false
    d.offset = Some(10)

    val exec = new CSVOldScanSourceOpExec(objectMapper.writeValueAsString(d))
    exec.open()
    val engineRows =
      try exec.produceTuple().toList
      finally exec.close()
    engineRows shouldBe empty

    val driver =
      s"""import pandas as pd
         |${d.standaloneImports().mkString("\n")}
         |${d.standaloneHelpers().mkString("\n")}
         |sourceFile = ${objectMapper.writeValueAsString(path)}
         |${d.generateStandaloneCode()}
         |print(len(out1df), list(out1df.columns))
         |""".stripMargin
    val script = Files.createTempFile("csv-old-standalone-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.trim shouldBe "0 ['column-1', 'column-2']"
      d.sourceSchema().getAttributeNames shouldBe List("column-1", "column-2")
    }
  }

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

  private def writeCsv(content: String): String = {
    val file = Files.createTempFile("csv-old-", ".csv")
    file.toFile.deleteOnExit()
    Files.write(file, content.getBytes(StandardCharsets.UTF_8))
    file.toString
  }

  private def describing(path: String): CSVOldScanSourceOpDesc = {
    val d = new CSVOldScanSourceOpDesc
    d.fileName = Some(path)
    d.customDelimiter = Some(",")
    d.hasHeader = true
    d.setResolvedFileName(FileResolver.resolve(path))
    d
  }
}
