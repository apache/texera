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
import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.core.tuple.{AttributeType, SchemaEnforceable}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
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

    // The body names its file by placeholder and the translator puts a name
    // there. Standing in for the translator is all this test needs, and the
    // process runs from the file's own directory, so the bare name resolves.
    val bindSourceFile =
      s"""${StandaloneCodeGenerator.SourceFilePlaceholder} = "${op.standaloneSourceName().get}""""

    val script = dir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${op.standaloneImports().mkString("\n")}
         |${op.standaloneHelpers().mkString("\n\n")}
         |$bindSourceFile
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

  // read_json parses a JSON number into a float before any dtype it is handed can
  // apply, so a long past 2^53 arrives already rounded: 9007199254740993 came back
  // as ...992, a value the file never held and the executor never produced. The
  // hole is what forces the widening, so the column needs one to show it.
  it should "keep a nullable long exact, as the executor does" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("jsonl-long-")
    dir.toFile.deleteOnExit()
    val data = dir.resolve("input.jsonl")
    Files.write(
      data,
      ("""{"id":1,"big":9007199254740993}""" + "\n" +
        """{"id":2}""" + "\n" +
        """{"id":3,"big":9007199254740995}""" + "\n").getBytes(StandardCharsets.UTF_8)
    )

    val op = new JSONLScanSourceOpDesc
    op.fileName = Some(data.toString)
    op.setResolvedFileName(FileResolver.resolve(data.toString))
    op.sourceSchema().getAttribute("big").getType shouldBe AttributeType.LONG

    val exec = new JSONLScanSourceOpExec(objectMapper.writeValueAsString(op))
    exec.open()
    val fromEngine =
      try exec
        .produceTuple()
        .map(
          _.asInstanceOf[SchemaEnforceable].enforceSchema(op.sourceSchema()).getField[Any]("big")
        )
        .toList
      finally exec.close()
    fromEngine shouldBe List(9007199254740993L, null, 9007199254740995L)

    val script = dir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${op.standaloneImports().mkString("\n")}
         |${op.standaloneHelpers().mkString("\n\n")}
         |${StandaloneCodeGenerator.SourceFilePlaceholder} = "${op.standaloneSourceName().get}"
         |${op.generateStandaloneCode()}
         |print([None if pd.isna(v) else int(v) for v in out1df["big"]])
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
      out.trim should endWith("[9007199254740993, None, 9007199254740995]")
    }
  }

  // The executor gives every element of a nested array a column of its own,
  // named for its position counted from one, so {"items":[{"id":1},{"id":2}]}
  // is items1.id and items2.id. json_normalize opens an object and leaves an
  // array whole, so the export handed the plan one items column holding a list
  // and a step reading items1.id found no such column.
  it should "name a flattened array's columns the way the executor does" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("jsonl-flatten-")
    dir.toFile.deleteOnExit()
    val data = dir.resolve("input.jsonl")
    Files.write(
      data,
      ("""{"items":[{"id":1},{"id":2}],"tags":["x","y"],"name":"a"}""" + "\n" +
        """{"items":[{"id":3},{"id":4}],"tags":["z"],"name":"b"}""" + "\n")
        .getBytes(StandardCharsets.UTF_8)
    )

    val op = new JSONLScanSourceOpDesc
    op.fileName = Some(data.toString)
    op.setResolvedFileName(FileResolver.resolve(data.toString))
    op.flatten = true
    val schema = op.sourceSchema()
    schema.getAttributeNames should contain allOf ("items1.id", "items2.id", "tags1", "tags2")

    val exec = new JSONLScanSourceOpExec(objectMapper.writeValueAsString(op))
    exec.open()
    val fromEngine =
      try exec
        .produceTuple()
        .map(_.asInstanceOf[SchemaEnforceable].enforceSchema(schema).getField[Any]("items2.id"))
        .toList
      finally exec.close()
    fromEngine shouldBe List(2, 4)

    val script = dir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${op.standaloneImports().mkString("\n")}
         |${op.standaloneHelpers().mkString("\n\n")}
         |${StandaloneCodeGenerator.SourceFilePlaceholder} = "${op.standaloneSourceName().get}"
         |${op.generateStandaloneCode()}
         |print(sorted(out1df.columns))
         |print(list(out1df["items2.id"]))
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
      // The columns the schema declares, and no items column standing for them.
      out should include(s"${schema.getAttributeNames.sorted.mkString("['", "', '", "']")}")
      out.trim should endWith("[2, 4]")
    }
  }

  // A JSONL file states no column order, so the operator sorts the names it
  // found and the rows the workflow sees follow that. read_json keeps the order
  // the first record used, which left the export writing the same columns in
  // another order than the run did.
  it should "order its columns the way the schema it infers does" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("jsonl-order-")
    dir.toFile.deleteOnExit()
    val data = dir.resolve("input.jsonl")
    Files.write(
      data,
      ("""{"zeta":1,"alpha":"a","mid":2.5}""" + "\n" +
        """{"zeta":2,"alpha":"b","mid":3.5}""" + "\n").getBytes(StandardCharsets.UTF_8)
    )

    val op = new JSONLScanSourceOpDesc
    op.fileName = Some(data.toString)
    op.setResolvedFileName(FileResolver.resolve(data.toString))
    val declared = op.sourceSchema().getAttributeNames
    declared shouldBe List("alpha", "mid", "zeta")

    val script = dir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${op.standaloneImports().mkString("\n")}
         |${op.standaloneHelpers().mkString("\n\n")}
         |${StandaloneCodeGenerator.SourceFilePlaceholder} = "${op.standaloneSourceName().get}"
         |${op.generateStandaloneCode()}
         |print(list(out1df.columns))
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
      out.trim should endWith(declared.mkString("['", "', '", "']"))
    }
  }

  // The exact re-read a long needs looks its column up by name, and under
  // flattening that name belongs to the flattened record rather than to the one
  // the file holds.
  it should "keep a nullable long inside a flattened array exact" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("jsonl-flatten-long-")
    dir.toFile.deleteOnExit()
    val data = dir.resolve("input.jsonl")
    Files.write(
      data,
      ("""{"items":[{"id":9007199254740993}]}""" + "\n" +
        """{"items":[{}]}""" + "\n" +
        """{"items":[{"id":9007199254740995}]}""" + "\n").getBytes(StandardCharsets.UTF_8)
    )

    val op = new JSONLScanSourceOpDesc
    op.fileName = Some(data.toString)
    op.setResolvedFileName(FileResolver.resolve(data.toString))
    op.flatten = true
    op.sourceSchema().getAttribute("items1.id").getType shouldBe AttributeType.LONG

    val script = dir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${op.standaloneImports().mkString("\n")}
         |${op.standaloneHelpers().mkString("\n\n")}
         |${StandaloneCodeGenerator.SourceFilePlaceholder} = "${op.standaloneSourceName().get}"
         |${op.generateStandaloneCode()}
         |print([None if pd.isna(v) else int(v) for v in out1df["items1.id"]])
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
      out.trim should endWith("[9007199254740993, None, 9007199254740995]")
    }
  }

  // read_json is handed the name the flattening will give a nested value, so it
  // looks for a column the file does not yet have and converts nothing. The
  // value stayed text, and a plan sorting on it read January 2025 as coming
  // before March 2024.
  it should "give a flattened timestamp the type the schema declares" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("jsonl-flatten-date-")
    dir.toFile.deleteOnExit()
    val data = dir.resolve("input.jsonl")
    // Written so that the text and the instant disagree on the order: as text
    // "01/15/2025" comes first, as a moment "03/01/2024" does.
    Files.write(
      data,
      ("""{"id":1,"meta":{"when":"03/01/2024 10:00:00"}}""" + "\n" +
        """{"id":2,"meta":{"when":"01/15/2025 08:30:00"}}""" + "\n")
        .getBytes(StandardCharsets.UTF_8)
    )

    val op = new JSONLScanSourceOpDesc
    op.fileName = Some(data.toString)
    op.setResolvedFileName(FileResolver.resolve(data.toString))
    op.flatten = true
    val schema = op.sourceSchema()
    schema.getAttribute("meta.when").getType shouldBe AttributeType.TIMESTAMP

    val exec = new JSONLScanSourceOpExec(objectMapper.writeValueAsString(op))
    exec.open()
    val fromEngine =
      try exec
        .produceTuple()
        .map(_.asInstanceOf[SchemaEnforceable].enforceSchema(schema))
        .toList
        .sortBy(_.getField[java.sql.Timestamp]("meta.when").getTime)
        .map(_.getField[Any]("id"))
      finally exec.close()
    fromEngine shouldBe List(1, 2)

    val script = dir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${op.standaloneImports().mkString("\n")}
         |${op.standaloneHelpers().mkString("\n\n")}
         |${StandaloneCodeGenerator.SourceFilePlaceholder} = "${op.standaloneSourceName().get}"
         |${op.generateStandaloneCode()}
         |print(out1df["meta.when"].dtype.kind)
         |print(list(out1df.sort_values("meta.when")["id"]))
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
      // "M" is a datetime column; text would print "O".
      out.linesIterator.map(_.trim).toList should contain("M")
      out.trim should endWith("[1, 2]")
    }
  }

  // The limit bounded the sample the inference reads as well as the rows the
  // operator emits, so a Limit of 0 had nothing to infer from and the operator
  // declared a schema of no columns at all. A file's columns do not depend on
  // how many of its rows were asked for.
  it should "keep the file's columns when the window asks for no rows" in {
    val data = Files.createTempFile("jsonl-zero-window-", ".jsonl")
    data.toFile.deleteOnExit()
    Files.write(
      data,
      "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n".getBytes(
        StandardCharsets.UTF_8
      )
    )

    val op = new JSONLScanSourceOpDesc
    op.fileName = Some(data.toString)
    op.setResolvedFileName(FileResolver.resolve(data.toString))
    op.limit = Some(0)

    val schema = op.sourceSchema()
    schema.getAttributeNames shouldBe List("id", "name")
    schema.getAttribute("id").getType shouldBe AttributeType.INTEGER
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
