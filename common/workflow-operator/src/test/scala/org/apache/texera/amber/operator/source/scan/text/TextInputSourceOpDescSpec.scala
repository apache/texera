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

package org.apache.texera.amber.operator.source.scan.text

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{
  AttributeType,
  AttributeTypeUtils,
  Schema,
  SchemaEnforceable,
  Tuple
}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.source.scan.FileAttributeType
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class TextInputSourceOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var textInputSourceOpDesc: TextInputSourceOpDesc = _

  before {
    textInputSourceOpDesc = new TextInputSourceOpDesc()
  }

  it should "infer schema with single column representing each line of text in normal text scan mode" in {
    val inferredSchema: Schema = textInputSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "infer schema with single column representing entire input in outputAsSingleTuple mode" in {
    textInputSourceOpDesc.attributeType = FileAttributeType.SINGLE_STRING
    val inferredSchema: Schema = textInputSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "infer schema with user-specified output schema attribute" in {
    textInputSourceOpDesc.attributeType = FileAttributeType.STRING
    val customOutputAttributeName: String = "testing"
    textInputSourceOpDesc.attributeName = customOutputAttributeName
    val inferredSchema: Schema = textInputSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("testing").getType == AttributeType.STRING)
  }

  it should "infer schema with integer attribute type" in {
    textInputSourceOpDesc.attributeType = FileAttributeType.INTEGER
    val inferredSchema: Schema = textInputSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.INTEGER)
  }

  it should "read first 5 lines of the input text into corresponding output tuples" in {
    val inputString: String = readFileIntoString(TestOperators.TestTextFilePath)
    textInputSourceOpDesc.attributeType = FileAttributeType.STRING
    textInputSourceOpDesc.textInput = inputString
    textInputSourceOpDesc.fileScanLimit = Option(5)
    val textScanSourceOpExec =
      new TextInputSourceOpExec(objectMapper.writeValueAsString(textInputSourceOpDesc))
    textScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = textScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike
          .asInstanceOf[SchemaEnforceable]
          .enforceSchema(textInputSourceOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    textScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text with CRLF separators into corresponding output tuples" in {
    val inputString: String = readFileIntoString(TestOperators.TestCRLFTextFilePath)
    textInputSourceOpDesc.attributeType = FileAttributeType.STRING
    textInputSourceOpDesc.textInput = inputString
    textInputSourceOpDesc.fileScanLimit = Option(5)
    val textScanSourceOpExec =
      new TextInputSourceOpExec(objectMapper.writeValueAsString(textInputSourceOpDesc))
    textScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = textScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike
          .asInstanceOf[SchemaEnforceable]
          .enforceSchema(textInputSourceOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    textScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text into a single output tuple" in {
    val inputString: String = readFileIntoString(TestOperators.TestTextFilePath)
    textInputSourceOpDesc.attributeType = FileAttributeType.SINGLE_STRING
    textInputSourceOpDesc.textInput = inputString
    val textScanSourceOpExec =
      new TextInputSourceOpExec(objectMapper.writeValueAsString(textInputSourceOpDesc))
    textScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = textScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike
          .asInstanceOf[SchemaEnforceable]
          .enforceSchema(textInputSourceOpDesc.sourceSchema())
      )

    assert(
      processedTuple
        .next()
        .getField[String]("line")
        .equals("line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\nline9\nline10")
    )
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    textScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text into corresponding output INTEGER tuples" in {
    val inputString: String = readFileIntoString(TestOperators.TestNumbersFilePath)
    textInputSourceOpDesc.attributeType = FileAttributeType.INTEGER
    textInputSourceOpDesc.textInput = inputString
    textInputSourceOpDesc.fileScanLimit = Option(5)
    val textScanSourceOpExec =
      new TextInputSourceOpExec(objectMapper.writeValueAsString(textInputSourceOpDesc))
    textScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = textScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike
          .asInstanceOf[SchemaEnforceable]
          .enforceSchema(textInputSourceOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField[Int]("line") == 1)
    assert(processedTuple.next().getField[Int]("line") == 2)
    assert(processedTuple.next().getField[Int]("line") == 3)
    assert(processedTuple.next().getField[Int]("line") == 4)
    assert(processedTuple.next().getField[Int]("line") == 5)
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    textScanSourceOpExec.close()
  }

  it should "read all lines after the offset when no limit is specified" in {
    assert(
      linesFrom(offset = Some(5)) == Seq("line6", "line7", "line8", "line9", "line10")
    )
  }

  it should "read all lines after the offset when the limit is Int.MaxValue" in {
    assert(
      linesFrom(offset = Some(1), limit = Some(Int.MaxValue)) ==
        Seq("line2", "line3", "line4", "line5", "line6", "line7", "line8", "line9", "line10")
    )
  }

  it should "read a window of lines when both offset and limit are specified" in {
    assert(linesFrom(offset = Some(5), limit = Some(2)) == Seq("line6", "line7"))
  }

  it should "read the first lines when only a limit is specified" in {
    assert(linesFrom(limit = Some(3)) == Seq("line1", "line2", "line3"))
  }

  it should "produce no tuples when the offset is at or past the end of the input" in {
    assert(linesFrom(offset = Some(10)).isEmpty)
    assert(linesFrom(offset = Some(99)).isEmpty)
  }

  it should "treat a negative offset as zero" in {
    assert(
      linesFrom(offset = Some(-1)) ==
        Seq(
          "line1",
          "line2",
          "line3",
          "line4",
          "line5",
          "line6",
          "line7",
          "line8",
          "line9",
          "line10"
        )
    )
  }

  it should "ignore the offset when reading the input text into a single output tuple" in {
    val inputString: String = readFileIntoString(TestOperators.TestTextFilePath)
    textInputSourceOpDesc.attributeType = FileAttributeType.SINGLE_STRING
    textInputSourceOpDesc.textInput = inputString
    textInputSourceOpDesc.fileScanOffset = Option(5)
    val textScanSourceOpExec =
      new TextInputSourceOpExec(objectMapper.writeValueAsString(textInputSourceOpDesc))
    textScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = textScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike
          .asInstanceOf[SchemaEnforceable]
          .enforceSchema(textInputSourceOpDesc.sourceSchema())
      )

    assert(
      processedTuple
        .next()
        .getField[String]("line")
        .equals("line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\nline9\nline10")
    )
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    textScanSourceOpExec.close()
  }

  /**
    * Helper function collecting the "line" field of every tuple produced for
    * the STRING attribute type with the given offset and limit.
    */
  private def linesFrom(offset: Option[Int] = None, limit: Option[Int] = None): Seq[String] = {
    textInputSourceOpDesc.attributeType = FileAttributeType.STRING
    textInputSourceOpDesc.textInput = readFileIntoString(TestOperators.TestTextFilePath)
    textInputSourceOpDesc.fileScanOffset = offset
    textInputSourceOpDesc.fileScanLimit = limit
    val exec = new TextInputSourceOpExec(objectMapper.writeValueAsString(textInputSourceOpDesc))
    exec.open()
    try {
      exec
        .produceTuple()
        .map(
          _.asInstanceOf[SchemaEnforceable]
            .enforceSchema(textInputSourceOpDesc.sourceSchema())
            .getField[String]("line")
        )
        .toSeq
    } finally exec.close()
  }

  /**
    * Helper function using UTF-8 encoding to read text file
    * into String
    *
    * @param filePath path of input file
    * @return entire file represented as String
    */
  def readFileIntoString(filePath: String): String = {
    val path: Path = Paths.get(filePath)
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
  }

  "TextInputSourceOpDesc.generateStandaloneCode" should
    "slice the raw lines before converting them" in {
    textInputSourceOpDesc.attributeType = FileAttributeType.INTEGER
    textInputSourceOpDesc.textInput = "1\n2\n3"

    textInputSourceOpDesc.fileScanOffset = Option(3)
    textInputSourceOpDesc.fileScanLimit = None
    assert(
      textInputSourceOpDesc
        .generateStandaloneCode()
        .endsWith("""{"line": [int(l) for l in _text.splitlines()[3:]]})""")
    )

    textInputSourceOpDesc.fileScanOffset = None
    textInputSourceOpDesc.fileScanLimit = Option(5)
    assert(
      textInputSourceOpDesc
        .generateStandaloneCode()
        .endsWith("""{"line": [int(l) for l in _text.splitlines()[:5]]})""")
    )

    textInputSourceOpDesc.fileScanOffset = Option(3)
    textInputSourceOpDesc.fileScanLimit = Option(5)
    assert(
      textInputSourceOpDesc
        .generateStandaloneCode()
        .endsWith("""{"line": [int(l) for l in _text.splitlines()[3:][:5]]})""")
    )
  }

  it should "parse a boolean line through the shared helper, and declare it" in {
    textInputSourceOpDesc.attributeType = FileAttributeType.BOOLEAN
    textInputSourceOpDesc.textInput = "true"
    assert(textInputSourceOpDesc.generateStandaloneCode().contains("_texera_parse_bool(l)"))
    assert(textInputSourceOpDesc.standaloneHelpers() == Seq(TextSourceOpDesc.BooleanParser))

    textInputSourceOpDesc.attributeType = FileAttributeType.STRING
    assert(textInputSourceOpDesc.standaloneHelpers().isEmpty)
  }

  // `parseField` reads a BOOLEAN line as "true" or "false" in any case, then as an
  // integer that is true only at 1, and refuses anything else. Comparing the
  // lowercased line to "true" called 1 false and passed text the engine refuses
  // off as a row of false.
  it should "read a boolean line the way the engine does" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    textInputSourceOpDesc.attributeType = FileAttributeType.BOOLEAN
    textInputSourceOpDesc.textInput = Seq("true", "TRUE", " true ", "False", "1", "0", "2", "-1")
      .mkString("\n")

    val expected = booleansFromEngine().map(b => if (b) "True" else "False").mkString(", ")
    val (exitCode, out) = runStandalone(python)
    withClue(s"python said:\n$out\n") {
      assert(exitCode == 0)
      assert(out.trim.endsWith(s"[$expected]"))
    }
  }

  it should "refuse a boolean line the engine refuses" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    textInputSourceOpDesc.attributeType = FileAttributeType.BOOLEAN
    textInputSourceOpDesc.textInput = "true\nyes"

    assertThrows[AttributeTypeUtils.AttributeTypeException](booleansFromEngine())
    val (exitCode, out) = runStandalone(python)
    withClue(s"python said:\n$out\n") {
      assert(exitCode != 0)
      assert(out.contains("ValueError"))
    }
  }

  /** The configured text read as booleans by the executor the engine runs. */
  private def booleansFromEngine(): Seq[Boolean] = {
    val exec = new TextInputSourceOpExec(objectMapper.writeValueAsString(textInputSourceOpDesc))
    exec.open()
    try {
      exec
        .produceTuple()
        .map(
          _.asInstanceOf[SchemaEnforceable]
            .enforceSchema(textInputSourceOpDesc.sourceSchema())
            .getField[Boolean]("line")
        )
        .toSeq
    } finally exec.close()
  }

  /**
    * The configured operator's exported script, run, printing its one column.
    *
    * `bool` on each value because a numpy scalar does not repr as Python's own
    * True and False.
    */
  private def runStandalone(python: String): (Int, String) = {
    val dir = Files.createTempDirectory("text-input-standalone-")
    dir.toFile.deleteOnExit()
    val script = dir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${textInputSourceOpDesc.standaloneHelpers().mkString("\n\n")}
         |${textInputSourceOpDesc.generateStandaloneCode()}
         |print([bool(v) for v in out1df["line"]])
         |""".stripMargin.getBytes(StandardCharsets.UTF_8)
    )

    val process = new ProcessBuilder(python, script.toString)
      .directory(dir.toFile)
      .redirectErrorStream(true)
      .start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    (process.exitValue(), out)
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

  "TextInputSourceOpDesc.getPhysicalOp" should
    "wire the TextInputSourceOpExec class as a source op with one output port" in {
    val physical =
      textInputSourceOpDesc.getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        assert(className == classOf[TextInputSourceOpExec].getName)
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    assert(physical.inputPorts.isEmpty)
    assert(physical.outputPorts.size == 1)
  }

  it should "propagate sourceSchema to its single output port" in {
    textInputSourceOpDesc.attributeType = FileAttributeType.STRING
    val physical =
      textInputSourceOpDesc.getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))
    val outPortId = textInputSourceOpDesc.operatorInfo.outputPorts.head.id
    val out = physical.propagateSchema.func(Map.empty)
    assert(out.keySet == Set(outPortId))
    assert(out(outPortId) == textInputSourceOpDesc.sourceSchema())
  }
}
