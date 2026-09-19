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

package org.apache.texera.amber.operator.source.scan.arrow

import com.typesafe.config.ConfigFactory
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  Float4Vector,
  SmallIntVector,
  TinyIntVector,
  UInt1Vector,
  UInt2Vector,
  UInt4Vector,
  UInt8Vector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema => ArrowFileSchema}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileOutputStream}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Try

class ArrowSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def writeArrowFile(schema: Schema, rows: Seq[Array[Any]]): File = {
    val file = File.createTempFile("arrow-src-", ".arrow")
    file.deleteOnExit()
    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(ArrowUtils.fromTexeraSchema(schema), allocator)
    val out = new FileOutputStream(file)
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))
    try {
      writer.start()
      root.allocateNew()
      rows.zipWithIndex.foreach {
        case (values, i) =>
          ArrowUtils.setTexeraTuple(Tuple.builder(schema).addSequentially(values).build(), i, root)
      }
      root.setRowCount(rows.size)
      writer.writeBatch()
      writer.end()
    } finally {
      writer.close()
      root.close()
      allocator.close()
      out.close()
    }
    file
  }

  /** A file holding the numeric widths [[ArrowUtils.fromTexeraSchema]] cannot
    * name.
    *
    * It writes every float as a double and every integer as a signed 32 or 64
    * bits, so a single-precision, narrow or unsigned column can only be built
    * from the Arrow schema itself. Real files carry them: anything pyarrow writes
    * from a numpy `float32`, `int16` or `uint32` array does.
    *
    * Each column holds the value its own width cannot be read out of by
    * accident, and one the reading has to leave alone.
    */
  private def writeNarrowArrowFile(): File = {
    val fields = List(
      new Field(
        "f",
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        null
      ),
      new Field("s", FieldType.nullable(new ArrowType.Int(16, true)), null),
      new Field("b", FieldType.nullable(new ArrowType.Int(8, true)), null),
      new Field("u8", FieldType.nullable(new ArrowType.Int(8, false)), null),
      new Field("u16", FieldType.nullable(new ArrowType.Int(16, false)), null),
      new Field("u32", FieldType.nullable(new ArrowType.Int(32, false)), null)
    )
    val file = File.createTempFile("arrow-narrow-", ".arrow")
    file.deleteOnExit()
    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(new ArrowFileSchema(fields.asJava), allocator)
    val out = new FileOutputStream(file)
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))
    try {
      writer.start()
      root.allocateNew()
      val f = root.getVector("f").asInstanceOf[Float4Vector]
      val s = root.getVector("s").asInstanceOf[SmallIntVector]
      val b = root.getVector("b").asInstanceOf[TinyIntVector]
      val u8 = root.getVector("u8").asInstanceOf[UInt1Vector]
      val u16 = root.getVector("u16").asInstanceOf[UInt2Vector]
      val u32 = root.getVector("u32").asInstanceOf[UInt4Vector]
      // 2^24 and 1: the pair a single-precision sum cannot tell from 2^24 alone.
      f.setSafe(0, 16777216.0f)
      f.setSafe(1, 1.0f)
      s.setSafe(0, 7.toShort)
      s.setSafe(1, 8.toShort)
      // A signed 8-bit column, whose negative must survive the unsigned reading.
      b.setSafe(0, (-3).toByte)
      b.setSafe(1, 4.toByte)
      // The largest value each unsigned width counts to, every one of them
      // stored as -1, beside a value the two readings agree on.
      u8.setSafe(0, 0xff)
      u8.setSafe(1, 1)
      u16.setSafe(0, 0xffff)
      u16.setSafe(1, 1)
      u32.setSafe(0, -1)
      u32.setSafe(1, 1)
      root.setRowCount(2)
      writer.writeBatch()
      writer.end()
    } finally {
      writer.close()
      root.close()
      allocator.close()
      out.close()
    }
    file
  }

  "ArrowSourceOpDesc.operatorInfo" should
    "advertise the Arrow file-scan name in the Data Input group with no input and one output" in {
    val info = (new ArrowSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Arrow File Scan"
    info.operatorDescription shouldBe "Scan data from an Arrow file"
    info.operatorGroupName shouldBe OperatorGroupConstants.INPUT_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "ArrowSourceOpDesc" should "default the encoding and scan window" in {
    val d = new ArrowSourceOpDesc
    d.fileName shouldBe None
    d.fileEncoding shouldBe FileDecodingMethod.UTF_8
    d.limit shouldBe None
    d.offset shouldBe None
    d.fileTypeName shouldBe Some("Arrow")
  }

  "ArrowSourceOpDesc.sourceSchema" should "be null before a file is resolved" in {
    (new ArrowSourceOpDesc).sourceSchema() shouldBe null
  }

  "ArrowSourceOpDesc.getPhysicalOp" should
    "wire the Arrow exec as a source op with no input port and one output port" in {
    val d = new ArrowSourceOpDesc
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.scan.arrow.ArrowSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "ArrowSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new ArrowSourceOpDesc
    d.fileName = Some("file:///tmp/data.arrow")
    d.limit = Some(7)
    d.offset = Some(3)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[ArrowSourceOpDesc]
    val r = restored.asInstanceOf[ArrowSourceOpDesc]
    r.fileName shouldBe Some("file:///tmp/data.arrow")
    r.limit shouldBe Some(7)
    r.offset shouldBe Some(3)
  }

  "ArrowSourceOpDesc.inferSchema" should "infer the Texera schema from a valid Arrow file" in {
    val schema = Schema(List(new Attribute("s", AttributeType.STRING)))
    val file = writeArrowFile(schema, Seq(Array[Any]("a"), Array[Any]("b")))
    val d = new ArrowSourceOpDesc
    d.fileName = Some(file.toURI.toString)
    val inferred = d.inferSchema()
    inferred.getAttributes should have length 1
    inferred.getAttributes.head.getName shouldBe "s"
    inferred.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  it should "infer every supported attribute type from a file containing null values" in {
    // Every AttributeType round-trips through Arrow (LARGE_BINARY/ANY are tagged in field
    // metadata). A single all-null row exercises the null-writing path for each type while
    // still producing a file whose schema spans all supported types. Exhaustive value/null
    // round-tripping itself is covered by ArrowUtilsSpec.
    val schema = Schema(
      List(
        new Attribute("i", AttributeType.INTEGER),
        new Attribute("l", AttributeType.LONG),
        new Attribute("d", AttributeType.DOUBLE),
        new Attribute("b", AttributeType.BOOLEAN),
        new Attribute("s", AttributeType.STRING),
        new Attribute("t", AttributeType.TIMESTAMP),
        new Attribute("bin", AttributeType.BINARY),
        new Attribute("lbin", AttributeType.LARGE_BINARY),
        new Attribute("any", AttributeType.ANY)
      )
    )
    val nullRow = Array.fill[Any](schema.getAttributes.length)(null)
    val file = writeArrowFile(schema, Seq(nullRow))
    val d = new ArrowSourceOpDesc
    d.fileName = Some(file.toURI.toString)
    d.inferSchema() shouldBe schema
  }

  // A missing double and a stored NaN are two different values to the engine,
  // and a numpy column has one slot for both. A holed integer column loses its
  // type the same way.
  "ArrowSourceOpDesc.generateStandaloneCode" should
    "keep a missing value apart from a NaN, and an integer integral" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val schema = Schema(
      List(new Attribute("d", AttributeType.DOUBLE), new Attribute("i", AttributeType.INTEGER))
    )
    val file = writeArrowFile(
      schema,
      Seq(
        Array[Any](null, null),
        Array[Any](Double.box(Double.NaN), Int.box(7))
      )
    )

    val d = new ArrowSourceOpDesc
    d.fileName = Some(file.toURI.toString)
    // The block reads the file from the script's own directory, so run there.
    val workDir = Files.createTempDirectory("arrow-standalone-")
    workDir.toFile.deleteOnExit()
    val beside = workDir.resolve(file.getName)
    Files.copy(file.toPath, beside)

    val script = workDir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${bindSourceFile(d)}
         |${d.generateStandaloneCode()}
         |print(str(out1df["d"].dtype), str(out1df["i"].dtype))
         |print(repr(out1df["d"].iloc[0]), repr(out1df["d"].iloc[1]))
         |""".stripMargin.getBytes(StandardCharsets.UTF_8)
    )

    val process = new ProcessBuilder(python, script.toString)
      .directory(workDir.toFile)
      .redirectErrorStream(true)
      .start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out") {
      process.exitValue() shouldBe 0
      val lines = out.trim.linesIterator.toSeq
      lines.head shouldBe "Float64 Int32"
      // The first is the value that was missing, the second the NaN that was
      // stored. Under numpy dtypes both printed as nan.
      lines(1) should include("<NA>")
      lines(1) should include("nan")
    }
  }

  // Only the property editor refuses a negative window; a plan posted to the API
  // arrives with one intact. `iloc` counts a negative bound from the end, so -1
  // asked for the last row where the executor's drop skips none, and for all but
  // the last where its take keeps none.
  it should "take a negative window from the front, as the executor reads it" in {
    val d = new ArrowSourceOpDesc
    d.offset = Some(-1)
    d.limit = Some(-1)

    d.generateStandaloneCode() should include("out1df.iloc[0:0]")
  }

  // Both bounds are Ints the operator accepts, and their sum is not one. Added
  // as Ints the window ended at -2, which `iloc` reads from the end: it asked
  // for everything but the last two rows where the executor takes every row
  // from the offset on.
  it should "count the end of the window past what an Int holds" in {
    val d = new ArrowSourceOpDesc
    d.offset = Some(Int.MaxValue)
    d.limit = Some(Int.MaxValue)

    d.generateStandaloneCode() should include("out1df.iloc[2147483647:4294967294]")
  }

  it should "throw a friendly error when the file is not a valid Arrow file" in {
    val bogus = File.createTempFile("not-arrow-", ".arrow")
    bogus.deleteOnExit()
    Files.write(bogus.toPath, "this is not arrow".getBytes)
    val d = new ArrowSourceOpDesc
    d.fileName = Some(bogus.toURI.toString)
    val ex = intercept[RuntimeException](d.inferSchema())
    ex.getMessage shouldBe "Failed to read the .arrow file. Please ensure it is a valid Arrow file."
  }

  // The same nullable dtypes keep a long exact: carried in a float, every value
  // past 2^53 is rounded, and the file's 9007199254740993 read back as ...992
  // where the executor hands the exact value on.
  it should "keep a nullable long exact, as the executor does" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val schema = Schema(List(new Attribute("big", AttributeType.LONG)))
    val file = writeArrowFile(
      schema,
      Seq(Array[Any](9007199254740993L), Array[Any](null), Array[Any](9007199254740995L))
    )

    val d = new ArrowSourceOpDesc
    d.fileName = Some(file.toURI.toString)

    val exec = new ArrowSourceOpExec(objectMapper.writeValueAsString(d))
    exec.open()
    val fromEngine =
      try exec.produceTuple().map(_.getFields.head).toList
      finally exec.close()
    fromEngine shouldBe List(9007199254740993L, null, 9007199254740995L)

    val workDir = Files.createTempDirectory("arrow-long-")
    workDir.toFile.deleteOnExit()
    Files.copy(file.toPath, workDir.resolve(file.getName))

    val script = workDir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${bindSourceFile(d)}
         |${d.generateStandaloneCode()}
         |print([None if pd.isna(v) else int(v) for v in out1df["big"]])
         |""".stripMargin.getBytes(StandardCharsets.UTF_8)
    )

    val process = new ProcessBuilder(python, script.toString)
      .directory(workDir.toFile)
      .redirectErrorStream(true)
      .start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out") {
      process.exitValue() shouldBe 0
      out.trim should endWith("[9007199254740993, None, 9007199254740995]")
    }
  }

  // A file states the width and the sign of each of its numbers, and Texera has
  // no column narrower than a double or a 32-bit integer. Both sides have to land
  // on the Texera one: the executor was reading a whole single-precision, narrow
  // or unsigned column as null, or as the -1 its storage counts down to, and the
  // script was keeping a width whose arithmetic gives another answer.
  it should "read the numeric widths and signs as the Texera column, on both paths" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val file = writeNarrowArrowFile()
    val d = new ArrowSourceOpDesc
    d.fileName = Some(file.toURI.toString)

    // An unsigned 32-bit column is a LONG: its largest value is past what an
    // INTEGER holds. Every narrower width fits in one.
    d.inferSchema().getAttributes.map(a => (a.getName, a.getType)) shouldBe List(
      ("f", AttributeType.DOUBLE),
      ("s", AttributeType.INTEGER),
      ("b", AttributeType.INTEGER),
      ("u8", AttributeType.INTEGER),
      ("u16", AttributeType.INTEGER),
      ("u32", AttributeType.LONG)
    )

    val exec = new ArrowSourceOpExec(objectMapper.writeValueAsString(d))
    exec.open()
    val fromEngine =
      try exec.produceTuple().map(_.getFields.toList).toList
      finally exec.close()
    fromEngine shouldBe List(
      List(16777216.0d, 7, -3, 255, 65535, 4294967295L),
      List(1.0d, 8, 4, 1, 1, 1L)
    )

    val workDir = Files.createTempDirectory("arrow-widths-")
    workDir.toFile.deleteOnExit()
    Files.copy(file.toPath, workDir.resolve(file.getName))

    val script = workDir.resolve("run.py")
    Files.write(
      script,
      s"""import pandas as pd
         |${bindSourceFile(d)}
         |${d.generateStandaloneCode()}
         |print(" ".join(str(out1df[c].dtype) for c in out1df.columns))
         |print([float(v) for v in out1df["f"]])
         |print([[int(v) for v in out1df[c]] for c in ("s", "b", "u8", "u16", "u32")])
         |# The pair the single-precision column could not add: kept as Float32
         |# this printed 16777216.0, where the executor's doubles give 16777217.0.
         |print(float(out1df["f"].sum()))
         |""".stripMargin.getBytes(StandardCharsets.UTF_8)
    )

    val process = new ProcessBuilder(python, script.toString)
      .directory(workDir.toFile)
      .redirectErrorStream(true)
      .start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out") {
      process.exitValue() shouldBe 0
      val lines = out.trim.linesIterator.toSeq
      lines.head shouldBe "Float64 Int32 Int32 Int32 Int32 Int64"
      lines(1) shouldBe "[16777216.0, 1.0]"
      lines(2) shouldBe "[[7, 8], [-3, 4], [255, 1], [65535, 1], [4294967295, 1]]"
      lines(3) shouldBe "16777217.0"
    }
  }

  // Nothing in Texera holds a value past 2^63 - 1, so the column is refused at
  // the schema rather than read as the -1 its storage counts down to.
  it should "refuse an unsigned 64-bit column, having no Texera type for it" in {
    val fields = List(new Field("u", FieldType.nullable(new ArrowType.Int(64, false)), null))
    val file = File.createTempFile("arrow-u64-", ".arrow")
    file.deleteOnExit()
    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(new ArrowFileSchema(fields.asJava), allocator)
    val out = new FileOutputStream(file)
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))
    try {
      writer.start()
      root.allocateNew()
      root.getVector("u").asInstanceOf[UInt8Vector].setSafe(0, -1L)
      root.setRowCount(1)
      writer.writeBatch()
      writer.end()
    } finally {
      writer.close()
      root.close()
      allocator.close()
      out.close()
    }

    val d = new ArrowSourceOpDesc
    d.fileName = Some(file.toURI.toString)
    val ex = intercept[RuntimeException](d.inferSchema())
    ex.getMessage shouldBe "Failed to read the .arrow file. Please ensure it is a valid Arrow file."
  }

  /** The block names its file by placeholder; the translator puts a name there. */
  private def bindSourceFile(d: ArrowSourceOpDesc): String =
    s"""${StandaloneCodeGenerator.SourceFilePlaceholder} = "${d.standaloneSourceName().get}""""

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
    ).toOption.exists { p =>
      if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
