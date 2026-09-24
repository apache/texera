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

package org.apache.texera.amber.operator.source.scan.file

import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, SchemaEnforceable, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.{StandaloneCodeGenerator, TestOperators}
import org.apache.texera.amber.operator.source.scan.{FileAttributeType, FileDecodingMethod}
import org.apache.texera.amber.operator.source.scan.text.TextSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.io.Source
import scala.util.Try

class FileScanSourceOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var fileScanSourceOpDesc: FileScanSourceOpDesc = _

  before {
    fileScanSourceOpDesc = new FileScanSourceOpDesc()
    fileScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(TestOperators.TestTextFilePath))
    fileScanSourceOpDesc.encoding = FileDecodingMethod.UTF_8
  }

  it should "infer schema with single column representing each line of text in normal text scan mode" in {
    val inferredSchema: Schema = fileScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "infer schema with single column representing entire file in outputAsSingleTuple mode" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.SINGLE_STRING
    val inferredSchema: Schema = fileScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "infer schema with user-specified output schema attribute" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    val customOutputAttributeName: String = "testing"
    fileScanSourceOpDesc.attributeName = customOutputAttributeName
    val inferredSchema: Schema = fileScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("testing").getType == AttributeType.STRING)
  }

  it should "infer schema with integer attribute type" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.INTEGER
    val inferredSchema: Schema = fileScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.INTEGER)
  }

  it should "read first 5 lines of the input text file into corresponding output tuples" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(objectMapper.writeValueAsString(fileScanSourceOpDesc))
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(fileScanSourceOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  it should "read the lines after a 5-line offset when no limit is set" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    fileScanSourceOpDesc.fileScanOffset = Option(5)
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(objectMapper.writeValueAsString(fileScanSourceOpDesc))
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(fileScanSourceOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField("line").equals("line6"))
    assert(processedTuple.next().getField("line").equals("line7"))
    assert(processedTuple.next().getField("line").equals("line8"))
    assert(processedTuple.next().getField("line").equals("line9"))
    assert(processedTuple.next().getField("line").equals("line10"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text file with CRLF separators into corresponding output tuples" in {
    fileScanSourceOpDesc.setResolvedFileName(
      FileResolver.resolve(TestOperators.TestCRLFTextFilePath)
    )
    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(objectMapper.writeValueAsString(fileScanSourceOpDesc))
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(fileScanSourceOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text file into a single output tuple" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.SINGLE_STRING
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(objectMapper.writeValueAsString(fileScanSourceOpDesc))
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(fileScanSourceOpDesc.sourceSchema())
      )

    assert(
      processedTuple
        .next()
        .getField("line")
        .equals("line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\nline9\nline10")
    )
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text into corresponding output INTEGER tuples" in {
    fileScanSourceOpDesc.setResolvedFileName(
      FileResolver.resolve(TestOperators.TestNumbersFilePath)
    )
    fileScanSourceOpDesc.attributeType = FileAttributeType.INTEGER
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(objectMapper.writeValueAsString(fileScanSourceOpDesc))
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(fileScanSourceOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField[Int]("line") == 1)
    assert(processedTuple.next().getField[Int]("line") == 2)
    assert(processedTuple.next().getField[Int]("line") == 3)
    assert(processedTuple.next().getField[Int]("line") == 4)
    assert(processedTuple.next().getField[Int]("line") == 5)
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  // `encoding` and not the inherited `fileEncoding`: the descriptor drops that
  // one on the way over, so setting it never reached the executor at all.
  it should "read first 5 lines of the input text file with US_ASCII encoding" in {
    fileScanSourceOpDesc = describing(
      Paths.get(TestOperators.TestCRLFTextFilePath),
      """"encoding":"US_ASCII""""
    )
    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(objectMapper.writeValueAsString(fileScanSourceOpDesc))
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec
      .produceTuple()
      .map(tupleLike =>
        tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(fileScanSourceOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  it should "carry the Encoding field through serialization into the executor" in {
    fileScanSourceOpDesc.encoding = FileDecodingMethod.UTF_16

    // getPhysicalOp hands the executor objectMapper.writeValueAsString(this), and
    // FileScanSourceOpExec reads the descriptor back out of that string, so the
    // charset only reaches the executor if it survives the round trip.
    val roundTripped = objectMapper.readValue(
      objectMapper.writeValueAsString(fileScanSourceOpDesc),
      classOf[FileScanSourceOpDesc]
    )

    assert(roundTripped.encoding == FileDecodingMethod.UTF_16)
  }

  it should "decode a UTF-16 file with the charset the Encoding field names" in {
    val utf16File = Files.createTempFile("file-scan-utf16", ".txt")
    try {
      Files.write(utf16File, "line1\nline2\nline3".getBytes(StandardCharsets.UTF_16))

      fileScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(utf16File.toString))
      fileScanSourceOpDesc.encoding = FileDecodingMethod.UTF_16
      fileScanSourceOpDesc.attributeType = FileAttributeType.STRING

      val fileScanSourceOpExec =
        new FileScanSourceOpExec(objectMapper.writeValueAsString(fileScanSourceOpDesc))
      fileScanSourceOpExec.open()
      val processedTuple: Iterator[Tuple] = fileScanSourceOpExec
        .produceTuple()
        .map(tupleLike =>
          tupleLike
            .asInstanceOf[SchemaEnforceable]
            .enforceSchema(fileScanSourceOpDesc.sourceSchema())
        )

      // Decoded as UTF-8 these bytes come back as the byte-order mark followed by
      // NUL-interleaved characters, so this is the assertion the old wiring failed.
      assert(processedTuple.next().getField("line").equals("line1"))
      assert(processedTuple.next().getField("line").equals("line2"))
      assert(processedTuple.next().getField("line").equals("line3"))
      assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
      fileScanSourceOpExec.close()
    } finally {
      Files.deleteIfExists(utf16File)
    }
  }

  "FileScanSourceOpDesc.generateStandaloneCode" should
    "slice the raw lines before converting them" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.INTEGER

    fileScanSourceOpDesc.fileScanOffset = Option(3)
    fileScanSourceOpDesc.fileScanLimit = None
    assert(
      fileScanSourceOpDesc
        .generateStandaloneCode()
        .contains("""{"line": [int(l.rstrip()) for l in _f.readlines()[3:]]}""")
    )

    fileScanSourceOpDesc.fileScanOffset = None
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    assert(
      fileScanSourceOpDesc
        .generateStandaloneCode()
        .contains("""{"line": [int(l.rstrip()) for l in _f.readlines()[:5]]}""")
    )

    fileScanSourceOpDesc.fileScanOffset = Option(3)
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    assert(
      fileScanSourceOpDesc
        .generateStandaloneCode()
        .contains("""{"line": [int(l.rstrip()) for l in _f.readlines()[3:][:5]]}""")
    )
  }

  it should "parse a boolean line through the shared helper, and declare it" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.BOOLEAN
    assert(fileScanSourceOpDesc.generateStandaloneCode().contains("_texera_parse_bool(l)"))
    assert(fileScanSourceOpDesc.standaloneHelpers() == Seq(TextSourceOpDesc.BooleanParser))

    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    assert(fileScanSourceOpDesc.standaloneHelpers().isEmpty)
  }

  "FileScanSourceOpDesc.getPhysicalOp" should
    "wire the FileScanSourceOpExec class as a source op and propagate its schema" in {
    val physical =
      fileScanSourceOpDesc.getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        assert(className == classOf[FileScanSourceOpExec].getName)
        assert(descString.nonEmpty)
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    assert(physical.inputPorts.isEmpty)
    val outPortId = fileScanSourceOpDesc.operatorInfo.outputPorts.head.id
    assert(physical.outputPorts.keySet == Set(outPortId))
    val propagated = physical.propagateSchema.func(Map.empty)
    assert(propagated(outPortId) == fileScanSourceOpDesc.sourceSchema())
  }

  "FileScanSourceOpDesc.sourceSchema" should
    "prepend a filename column when outputFileName is enabled" in {
    // outputFileName is a val; round-trip through JSON (which carries the operatorType
    // discriminator) with the flag flipped on, since it can't be set on an instance.
    val node =
      objectMapper
        .readTree(objectMapper.writeValueAsString(fileScanSourceOpDesc))
        .asInstanceOf[ObjectNode]
    node.put("outputFileName", true)
    val withFlag = objectMapper.treeToValue(node, classOf[FileScanSourceOpDesc])
    val schema: Schema = withFlag.sourceSchema()
    assert(schema.getAttributes.length == 2)
    assert(schema.getAttribute("filename").getType == AttributeType.STRING)
    assert(schema.getAttribute("line").getType == AttributeType.STRING)
  }

  // With extract on the engine reads the files INSIDE the archive. The export
  // used to open the archive itself and hand back whatever bytes the compression
  // left readable, behind a warning comment.
  "FileScanSourceOpDesc.generateStandaloneCode" should
    "read the same entries out of an archive as the engine does" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("file-scan-archive-")
    dir.toFile.deleteOnExit()
    val archive = writeArchive(
      dir,
      "a.txt" -> "line1\nline2\n",
      "b.txt" -> "line3\n",
      // macOS puts these beside the real entries; the engine skips them by name.
      "__MACOSX/a.txt" -> "junk\n"
    )

    val desc = extractingDesc(archive, """"attributeType":"string"""")
    assert(linesFromEngine(desc) == Seq("line1", "line2", "line3"))
    assert(
      runStandalone(python, dir, desc, """print(list(out1df["line"]))""")._2.trim
        .endsWith("""['line1', 'line2', 'line3']""")
    )
  }

  // The Encoding field the panel offers is `encoding`, and the executor was
  // decoding with the inherited `fileEncoding` this descriptor drops on the way
  // over, so every file came back read as UTF-8 whatever was chosen. A UTF-16
  // file is the one that shows it: read as UTF-8 its text is not its text.
  it should "decode with the charset the Encoding field names" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("file-scan-encoding-")
    dir.toFile.deleteOnExit()
    val file = dir.resolve("utf16.txt")
    Files.write(file, "première\ndeuxième\n".getBytes(StandardCharsets.UTF_16))

    val desc = describing(file, """"encoding":"UTF_16"""")
    assert(linesFromEngine(desc) == Seq("première", "deuxième"))

    val out = runStandalone(python, dir, desc, """print(list(out1df["line"]))""")._2
    assert(out.trim.endsWith("""['première', 'deuxième']"""))
  }

  it should "take an archive entry's own name for the filename column" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("file-scan-archive-named-")
    dir.toFile.deleteOnExit()
    val archive = writeArchive(dir, "a.txt" -> "first", "b.txt" -> "second")

    val desc = extractingDesc(
      archive,
      """"attributeType":"single string"""",
      """"outputFileName":true"""
    )
    val engine =
      tuplesFromEngine(desc).map(t => (t.getField[String]("filename"), t.getField[String]("line")))
    assert(engine == Seq(("a.txt", "first"), ("b.txt", "second")))

    val out = runStandalone(
      python,
      dir,
      desc,
      """print(list(out1df.itertuples(index=False, name=None)))"""
    )._2
    assert(out.trim.endsWith("""[('a.txt', 'first'), ('b.txt', 'second')]"""))
  }

  // Include Filename is only offered once Extract is on, and reading the entries
  // line by line is the default there, so this is the configuration the panel
  // invites first. The line-by-line branch carried no name, leaving a one-field
  // row against the two-column schema, and the first tuple could not be built.
  it should "name the entry each line came from, reading an archive line by line" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val dir = Files.createTempDirectory("file-scan-archive-lines-")
    dir.toFile.deleteOnExit()
    val archive = writeArchive(dir, "a.txt" -> "one\ntwo\n", "b.txt" -> "three\n")

    val desc = extractingDesc(archive, """"outputFileName":true""")
    val engine =
      tuplesFromEngine(desc).map(t => (t.getField[String]("filename"), t.getField[String]("line")))
    assert(engine == Seq(("a.txt", "one"), ("a.txt", "two"), ("b.txt", "three")))

    val out = runStandalone(
      python,
      dir,
      desc,
      """print(list(out1df.itertuples(index=False, name=None)))"""
    )._2
    assert(
      out.trim.endsWith("""[('a.txt', 'one'), ('a.txt', 'two'), ('b.txt', 'three')]""")
    )
  }

  /** A zip at `dir/archive.zip` holding the given entries. */
  private def writeArchive(dir: Path, entries: (String, String)*): Path = {
    val archive = dir.resolve("archive.zip")
    val out = new ZipOutputStream(Files.newOutputStream(archive))
    try entries.foreach {
      case (name, content) =>
        out.putNextEntry(new ZipEntry(name))
        out.write(content.getBytes(StandardCharsets.UTF_8))
        out.closeEntry()
    } finally out.close()
    archive
  }

  /** `extract`, `outputFileName` and `encoding` are vals, so the fields are
    * deserialized in.
    */
  private def describing(file: Path, fields: String*): FileScanSourceOpDesc = {
    val desc = objectMapper.readValue(
      (""""operatorType":"FileScan"""" +: fields).mkString("{", ",", "}"),
      classOf[FileScanSourceOpDesc]
    )
    desc.setResolvedFileName(FileResolver.resolve(file.toString))
    desc
  }

  private def extractingDesc(archive: Path, fields: String*): FileScanSourceOpDesc =
    describing(archive, """"extract":true""" +: fields: _*)

  private def tuplesFromEngine(desc: FileScanSourceOpDesc): Seq[Tuple] = {
    val exec = new FileScanSourceOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    try exec
      .produceTuple()
      .map(_.asInstanceOf[SchemaEnforceable].enforceSchema(desc.sourceSchema()))
      .toSeq
    finally exec.close()
  }

  private def linesFromEngine(desc: FileScanSourceOpDesc): Seq[String] =
    tuplesFromEngine(desc).map(_.getField[String]("line"))

  /**
    * The operator's exported body, run from `dir`, with the placeholder bound as
    * the translator binds it and the imports it declared written out.
    */
  private def runStandalone(
      python: String,
      dir: Path,
      desc: FileScanSourceOpDesc,
      tail: String
  ): (Int, String) = {
    val script = dir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${desc.standaloneImports().mkString("\n")}
         |${StandaloneCodeGenerator.SourceFilePlaceholder} = "${desc.standaloneSourceName().get}"
         |${desc.generateStandaloneCode()}
         |$tail
         |""".stripMargin.getBytes(StandardCharsets.UTF_8)
    )

    val process = new ProcessBuilder(python, script.toString)
      .directory(dir.toFile)
      .redirectErrorStream(true)
      .start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\n")(assert(process.exitValue() == 0))
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
}
