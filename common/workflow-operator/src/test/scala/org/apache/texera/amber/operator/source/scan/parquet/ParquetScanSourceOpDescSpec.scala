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

package org.apache.texera.amber.operator.source.scan.parquet

import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{NanoTime, SimpleGroupFactory}
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Type, Types}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, SeqTupleLike}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

class ParquetScanSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  /** A Parquet file over `messageType`, one row per `rows` entry. */
  private def writeFile(messageType: MessageType)(rows: (Group => Unit)*): File = {
    val file = File.createTempFile("parquet-src-", ".parquet")
    file.delete() // the writer refuses to overwrite
    file.deleteOnExit()
    val conf = new Configuration()
    GroupWriteSupport.setSchema(messageType, conf)
    val factory = new SimpleGroupFactory(messageType)
    Using(
      ExampleParquetWriter
        .builder(new HadoopPath(file.getAbsolutePath))
        .withConf(conf)
        .withType(messageType)
        .build()
    ) { writer =>
      rows.foreach { fill =>
        val row = factory.newGroup()
        fill(row)
        writer.write(row)
      }
    }.get
    file
  }

  private def descFor(file: File): ParquetScanSourceOpDesc = {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some(file.toURI.toString)
    d
  }

  /** The rows the executor reads, each as the cells it hands on. */
  private def readRows(d: ParquetScanSourceOpDesc): List[List[Any]] = {
    val exec = new ParquetScanSourceOpExec(objectMapper.writeValueAsString(d))
    exec.open()
    try exec.produceTuple().toList.map(_.asInstanceOf[SeqTupleLike].getFields.toList)
    finally exec.close()
  }

  /** A two-row file over the types the mapping treats specially. */
  private def sampleFile(): File =
    writeFile(
      Types
        .buildMessage()
        .addFields(
          Types.optional(PrimitiveTypeName.INT32).named("id"),
          Types
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name"),
          Types.optional(PrimitiveTypeName.DOUBLE).named("score"),
          Types
            .optional(PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("seen_at"),
          Types.optional(PrimitiveTypeName.BOOLEAN).named("active")
        )
        .named("sample")
    )(
      row => {
        row.append("id", 7)
        row.append("name", "alice")
        row.append("score", 1.5d)
        row.append(
          "seen_at",
          LocalDateTime.of(2024, 3, 2, 14, 5, 9).toInstant(ZoneOffset.UTC).toEpochMilli
        )
        row.append("active", true)
      },
      // Every column optional, so a second row can leave them all out. Parquet
      // has no null value: a field that repeats zero times is the hole.
      _ => ()
    )

  /** A one-row file over a single timestamp column, counted in `unit`. */
  private def timestampFile(unit: LogicalTypeAnnotation.TimeUnit, count: Long): File =
    writeFile(
      Types
        .buildMessage()
        .addField(
          Types
            .optional(PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, unit))
            .named("seen_at")
        )
        .named("sample")
    )(_.append("seen_at", count))

  /**
    * A one-row file over the same decimal written in each storage Parquet allows
    * for one: the integer 1234 with a scale of 2, which is 12.34.
    */
  private def decimalFile(): File = {
    val decimal = LogicalTypeAnnotation.decimalType(2, 9)
    writeFile(
      Types
        .buildMessage()
        .addFields(
          Types.optional(PrimitiveTypeName.INT32).as(decimal).named("in_int"),
          Types.optional(PrimitiveTypeName.INT64).as(decimal).named("in_long"),
          Types
            .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(4)
            .as(decimal)
            .named("in_bytes")
        )
        .named("sample")
    )(row => {
      row.append("in_int", 1234)
      row.append("in_long", 1234L)
      // The wide storages hold the integer as its big-endian two's complement.
      row.append("in_bytes", Binary.fromConstantByteArray(Array[Byte](0, 0, 4, -46)))
    })
  }

  private def unsignedColumn(name: String, bitWidth: Int): Type =
    Types
      .optional(PrimitiveTypeName.INT32)
      .as(LogicalTypeAnnotation.intType(bitWidth, false))
      .named(name)

  "ParquetScanSourceOpDesc.operatorInfo" should
    "advertise the Parquet file-scan name in the Data Input group with no input and one output" in {
    val info = (new ParquetScanSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Parquet File Scan"
    info.operatorDescription shouldBe "Scan data from a Parquet file"
    info.operatorGroupName shouldBe OperatorGroupConstants.INPUT_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  it should "default the scan window and declare its format" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName shouldBe None
    d.limit shouldBe None
    d.offset shouldBe None
    d.fileTypeName shouldBe Some("Parquet")
  }

  // The file states its own types, which is the reason to read one at all.
  "ParquetScanSourceOpDesc.inferSchema" should "take the columns from the file's footer" in {
    val schema = descFor(sampleFile()).inferSchema()
    schema.getAttributeNames shouldBe List("id", "name", "score", "seen_at", "active")
    schema.getAttribute("id").getType shouldBe AttributeType.INTEGER
    schema.getAttribute("name").getType shouldBe AttributeType.STRING
    schema.getAttribute("score").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("seen_at").getType shouldBe AttributeType.TIMESTAMP
    schema.getAttribute("active").getType shouldBe AttributeType.BOOLEAN
  }

  // A DECIMAL is an integer plus a scale. The integer is not the value, and a
  // column read as one would be off by a factor of ten per decimal place.
  it should "read a decimal column as the number it stands for" in {
    val d = descFor(decimalFile())
    d.inferSchema().getAttributes.map(_.getType) shouldBe
      List(AttributeType.DOUBLE, AttributeType.DOUBLE, AttributeType.DOUBLE)
    readRows(d).head shouldBe List(12.34d, 12.34d, 12.34d)
  }

  it should "refuse a file that has not been selected" in {
    a[IllegalArgumentException] should be thrownBy (new ParquetScanSourceOpDesc).inferSchema()
  }

  it should "say the file is not readable rather than fail obscurely" in {
    val notParquet = Files.createTempFile("parquet-src-", ".parquet")
    Files.write(notParquet, "id,name\n1,alice\n".getBytes)
    notParquet.toFile.deleteOnExit()
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some(notParquet.toUri.toString)
    val error = intercept[RuntimeException](d.inferSchema())
    error.getMessage should include("valid Parquet file")
  }

  // A Texera column holds one value, and a group holds several, so there is no
  // column for it to become. Refused by name beats dropped in silence.
  "ParquetSchemaMapping" should "refuse a nested column by name" in {
    val nested = Types
      .buildMessage()
      .addField(
        Types
          .optionalGroup()
          .addField(Types.optional(PrimitiveTypeName.INT32).named("inner"))
          .named("outer")
      )
      .named("sample")
    val error =
      intercept[UnsupportedOperationException](ParquetSchemaMapping.toTexeraSchema(nested))
    error.getMessage should include("'outer'")
  }

  // None of these is a value Texera has a column for, and pandas reads every one
  // of them as what the file means rather than as its storage: a list, a time of
  // day, a number past what a Texera integer holds. Read here as the storage,
  // the engine would answer one thing and the exported script another.
  it should "refuse by name the columns Texera has no value for" in {
    val refused = Seq(
      // A repeated column holds a list per row; only the first of them would be read.
      Types.repeated(PrimitiveTypeName.INT32).named("tags"),
      Types
        .optional(PrimitiveTypeName.INT32)
        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("opens_at"),
      Types
        .optional(PrimitiveTypeName.INT64)
        .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
        .named("closes_at"),
      // Texera's widest integer is signed, so the top half of this one has nowhere to go.
      Types
        .optional(PrimitiveTypeName.INT64)
        .as(LogicalTypeAnnotation.intType(64, false))
        .named("hits"),
      Types
        .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(12)
        .as(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
        .named("span"),
      Types
        .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(2)
        .as(LogicalTypeAnnotation.float16Type())
        .named("ratio")
    )

    refused.foreach { field =>
      val error = intercept[UnsupportedOperationException](
        ParquetSchemaMapping.toTexeraSchema(
          Types.buildMessage().addField(field).named("sample")
        )
      )
      error.getMessage should include(s"'${field.getName}'")
    }
  }

  "ParquetScanSourceOpDesc.getPhysicalOp" should
    "wire the Parquet exec as a source op with no input port and one output port" in {
    val d = descFor(sampleFile())
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe
          "org.apache.texera.amber.operator.source.scan.parquet.ParquetScanSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "ParquetScanSourceOpExec" should "read the values the file was written with" in {
    val rows = readRows(descFor(sampleFile()))
    rows should have length 2
    rows.head shouldBe List(
      7,
      "alice",
      1.5d,
      Timestamp.valueOf(LocalDateTime.of(2024, 3, 2, 14, 5, 9)),
      true
    )
    // The second row wrote no field at all, so every cell is a hole.
    rows(1) shouldBe List(null, null, null, null, null)
  }

  // The timestamp is the case a zone could quietly enter. The file counts from
  // the epoch; a Texera TIMESTAMP is a wall clock. Reading it with the machine's
  // own zone would move it, and the exported script would not agree.
  it should "read a timestamp as the wall clock the file counts to, in any zone" in {
    readRows(descFor(sampleFile())).head(3) shouldBe Timestamp.valueOf("2024-03-02 14:05:09")
  }

  // The file says what unit it counts in, and a Texera TIMESTAMP holds
  // nanoseconds, so the digits under the millisecond are the file's to keep.
  it should "keep the precision a timestamp was written with" in {
    val secondsFromEpoch =
      LocalDateTime.of(2023, 11, 14, 22, 13, 20).toEpochSecond(ZoneOffset.UTC)
    val cases = Seq(
      LogicalTypeAnnotation.TimeUnit.MICROS -> (secondsFromEpoch * 1000000L + 123456L),
      LogicalTypeAnnotation.TimeUnit.NANOS -> (secondsFromEpoch * 1000000000L + 123456789L)
    )
    val expected = Seq("2023-11-14 22:13:20.123456", "2023-11-14 22:13:20.123456789")

    cases.zip(expected).foreach {
      case ((unit, count), wallClock) =>
        readRows(descFor(timestampFile(unit, count))).head.head shouldBe
          Timestamp.valueOf(wallClock)
    }
  }

  // An unsigned column counts up where its storage counts down: the largest
  // unsigned 32-bit value is stored as -1, and a reader that took the storage
  // for the value would hand back -1 where pandas reads 4294967295.
  it should "read an unsigned column as the number the file counts to" in {
    val d = descFor(
      writeFile(
        Types
          .buildMessage()
          .addFields(
            unsignedColumn("small", 8),
            unsignedColumn("middling", 16),
            unsignedColumn("large", 32)
          )
          .named("sample")
      )(row => {
        row.append("small", 255) // the largest of each width, as the file stores it
        row.append("middling", 65535)
        row.append("large", -1)
      })
    )

    d.inferSchema().getAttributes.map(_.getType) shouldBe
      List(AttributeType.LONG, AttributeType.LONG, AttributeType.LONG)
    readRows(d).head shouldBe List(255L, 65535L, 4294967295L)
  }

  // Twelve bytes holding a Julian day and the nanoseconds into it, which is how
  // a timestamp was written before there was an annotation for one.
  it should "read the timestamp an older writer wrote in twelve bytes" in {
    val d = descFor(
      writeFile(
        Types
          .buildMessage()
          .addField(Types.optional(PrimitiveTypeName.INT96).named("seen_at"))
          .named("sample")
      )(
        _.append(
          "seen_at",
          // 2024-01-02 is Julian day 2460312; 03:04:05.123456789 into it.
          new NanoTime(2460312, 11045123456789L).toBinary
        )
      )
    )

    d.inferSchema().getAttribute("seen_at").getType shouldBe AttributeType.TIMESTAMP
    readRows(d).head.head shouldBe Timestamp.valueOf("2024-01-02 03:04:05.123456789")
  }

  // A JSON column is text that carries a grammar. The grammar is not Texera's to
  // keep, but the text is, and pandas reads that column as text as well.
  it should "read a JSON column as the text it holds" in {
    val d = descFor(
      writeFile(
        Types
          .buildMessage()
          .addField(
            Types
              .optional(PrimitiveTypeName.BINARY)
              .as(LogicalTypeAnnotation.jsonType())
              .named("payload")
          )
          .named("sample")
      )(_.append("payload", """{"a":1}"""))
    )

    d.inferSchema().getAttribute("payload").getType shouldBe AttributeType.STRING
    readRows(d).head.head shouldBe """{"a":1}"""
  }

  "ParquetScanSourceOpDesc.generateStandaloneCode" should "read the file by its own name" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some("file:///tmp/some%20dir/data.parquet")
    // The translator names the file, so that two sources reading different files
    // whose paths end alike do not both ask for "data.parquet".
    d.generateStandaloneCode() should startWith("out1df = pd.read_parquet(sourceFile,")
    d.standaloneSourcePath() shouldBe d.fileName
    d.standaloneSourceName() shouldBe Some("data.parquet")
  }

  // pandas fills a DECIMAL column with decimal.Decimal objects, which do not mix
  // with the floats the executor reads the same column as.
  it should "cast the columns pandas reads as decimals" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some("file:///tmp/data.parquet")
    d.standaloneImports() should contain("from decimal import Decimal")
    d.generateStandaloneCode() should include("""_values.astype("Float64")""")
  }

  // Parquet says of every value whether it is there, and a numpy column has
  // nowhere to put that: a holed integer column is widened through a float,
  // where 9007199254740993 comes back as ...992. The executor reads the exact
  // long off the same file.
  it should "read into the nullable dtypes, which keep a holed integer integral" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some("file:///tmp/data.parquet")
    d.generateStandaloneCode() should include("""dtype_backend="numpy_nullable"""")
  }

  // The rest of what pandas reads differently from the executor: a FLOAT keeps
  // its single precision, an unsigned column stays unsigned, and the Arrow types
  // the writer left in the file's metadata bring back a duration and a zone that
  // the footer alone does not state.
  it should "put back the columns pandas reads as something else" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some("file:///tmp/data.parquet")
    val code = d.generateStandaloneCode()
    code should include("""_values.dt.tz_convert("UTC").dt.tz_localize(None)""")
    code should include("""elif _values.dtype.kind in "um":""")
    // Named in the nullable spelling, the read now asking for those dtypes.
    code should include("""elif _values.dtype == "Float32":""")
  }

  // The executor drops `offset` rows and then takes `limit`, and the script has
  // to land on the same rows.
  it should "take the same window the executor takes" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some("file:///tmp/data.parquet")
    d.offset = Some(2)
    d.limit = Some(3)
    d.generateStandaloneCode() should include("out1df.iloc[2:5]")
    d.offset = None
    d.generateStandaloneCode() should include("out1df.iloc[:3]")
    d.limit = None
    d.offset = Some(4)
    d.generateStandaloneCode() should include("out1df.iloc[4:]")
    // Two Ints the panel accepts add up past what an Int holds. Counted in one,
    // the end of the window would come out negative and take the wrong rows.
    d.offset = Some(Int.MaxValue)
    d.limit = Some(10)
    d.generateStandaloneCode() should include(s"out1df.iloc[${Int.MaxValue}:2147483657]")
  }

  "ParquetScanSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some("file:///tmp/data.parquet")
    d.limit = Some(5)
    d.offset = Some(1)
    val restored = objectMapper
      .readValue(objectMapper.writeValueAsString(d: LogicalOp), classOf[LogicalOp])
      .asInstanceOf[ParquetScanSourceOpDesc]
    restored.fileName shouldBe d.fileName
    restored.limit shouldBe d.limit
    restored.offset shouldBe d.offset
  }

  // Binary formats state their own encoding, so the base class's charset knob is
  // meaningless here and is kept out of the serialized config.
  it should "not carry a file encoding" in {
    val d = new ParquetScanSourceOpDesc
    d.fileEncoding shouldBe FileDecodingMethod.UTF_8
    objectMapper.readTree(objectMapper.writeValueAsString(d)).fieldNames().asScala.toList should
      not contain "fileEncoding"
  }

  /**
    * The whole of the parity claim, run rather than asserted: the same file read
    * by the executor and by the script the export writes, cell for cell. A moment
    * is compared as its nanoseconds from the epoch and bytes as their numbers, so
    * neither side is read through the other's idea of how to print one.
    */
  it should "read the same values the exported script reads" in {
    val python = runnablePython().getOrElse(
      cancel("No runnable python with pandas and pyarrow (udf.conf python.path, python3, python)")
    )

    val file = writeFile(
      Types
        .buildMessage()
        .addFields(
          Types
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label"),
          Types
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.jsonType())
            .named("payload"),
          Types.optional(PrimitiveTypeName.BINARY).named("blob"),
          unsignedColumn("count", 32),
          Types
            .optional(PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("amount"),
          // The float the executor widens to a double. Stored as 16777216 and 1,
          // a column left in single precision sums to 16777216, not 16777217.
          Types.optional(PrimitiveTypeName.FLOAT).named("size"),
          Types.optional(PrimitiveTypeName.INT64).named("big"),
          Types
            .optional(PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("seen_at"),
          Types.optional(PrimitiveTypeName.INT96).named("legacy_at"),
          Types.optional(PrimitiveTypeName.BOOLEAN).named("active")
        )
        .named("sample")
    )(
      row => {
        row.append("label", "alice")
        row.append("payload", """{"a":1}""")
        row.append("blob", Binary.fromConstantByteArray(Array[Byte](1, 2, 3)))
        row.append("count", -1) // the largest unsigned 32-bit value, as it is stored
        row.append("amount", 1234)
        row.append("size", 16777216.0f)
        row.append("big", 9007199254740993L) // past what a double counts exactly
        row.append("seen_at", 1709388309123456L)
        row.append("legacy_at", new NanoTime(2460312, 11045123456789L).toBinary)
        row.append("active", true)
      },
      row => {
        row.append("label", "bob")
        row.append("payload", """{"a":2}""")
        row.append("blob", Binary.fromConstantByteArray(Array[Byte](4, 5, 6)))
        row.append("count", 0)
        row.append("amount", -1234)
        row.append("size", 1.0f)
        row.append("big", -9007199254740993L)
        row.append("seen_at", 0L)
        row.append("legacy_at", new NanoTime(2440588, 0L).toBinary)
        row.append("active", false)
      },
      // A hole in every column, which is what costs a numpy column its type: a
      // holed integer is widened through a float, and `big` above came back as
      // ...992. Every field is optional, so a row that appends nothing to one
      // leaves it null.
      row => {
        row.append("label", "carol")
      }
    )

    val d = descFor(file)
    val columns = d.inferSchema().getAttributeNames
    val rows = readRows(d)
    val fromExecutor = columns.zipWithIndex.map {
      case (column, index) => column -> rows.map(row => comparable(row(index)))
    }.toMap

    // The body names its file by placeholder and leaves the naming to whoever
    // assembles the script, the translator doing it across a whole plan. Nothing
    // bound it here, so the script stopped on a `sourceFile` that was never
    // defined and this comparison never ran.
    val bindFile = s"""sourceFile = "${d.standaloneSourceName().get}""""
    val script = (d.standaloneImports() :+ "import json" :+ "import pandas as pd" :+
      bindFile :+ d.generateStandaloneCode() :+ ParityDriver).mkString("\n")
    val scriptFile = Files.createTempFile("parquet_parity_", ".py")
    scriptFile.toFile.deleteOnExit()
    Files.write(scriptFile, script.getBytes(StandardCharsets.UTF_8))

    // The script reads the file by its own name, as the export writes it, so it
    // runs where the file is.
    val process = new ProcessBuilder(python, scriptFile.toString)
      .directory(file.getParentFile)
      .redirectErrorStream(true)
      .start()
    if (!process.waitFor(120, TimeUnit.SECONDS)) {
      process.destroyForcibly()
      fail("The exported script did not finish within 120s")
    }
    val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)

    withClue(s"Script output:\n$output\n") {
      process.exitValue() shouldBe 0
      val printed = output.linesIterator.find(_.startsWith("JSON ")).map(_.drop("JSON ".length))
      val fromScript = objectMapper
        .readTree(printed.getOrElse(fail("the script printed no row")))
        .asInstanceOf[ObjectNode]
      // The single precision the executor widens, stated as the sum it changes.
      fromScript.get("size_sum").doubleValue() shouldBe 16777217.0d
      fromScript.remove("size_sum")
      fromScript shouldBe objectMapper.readTree(objectMapper.writeValueAsString(fromExecutor))
    }
  }

  /** A cell as the two sides can be held to the same answer for it. */
  private def comparable(cell: Any): Any =
    cell match {
      case moment: Timestamp =>
        ChronoUnit.NANOS.between(Instant.EPOCH, moment.toLocalDateTime.toInstant(ZoneOffset.UTC))
      case bytes: Array[Byte] => bytes.map(_ & 0xff).toList
      case other              => other
    }

  /** Printed by the script run above, once the generated code has read the file. */
  private val ParityDriver: String =
    """|_cells = {}
       |for _column in out1df.columns:
       |    _values = []
       |    for _value in out1df[_column]:
       |        # Bytes first, since pd.isna reads a buffer element by element.
       |        if isinstance(_value, (bytes, bytearray)):
       |            _values.append(list(_value))
       |        # A hole: pd.NA in a nullable column and NaT in a timestamp one,
       |        # the first of which json refuses outright and the second of which
       |        # the branch below would write as the sentinel it holds. The
       |        # executor hands a null over, so that is what it is compared as.
       |        elif _value is None or pd.isna(_value):
       |            _values.append(None)
       |        elif hasattr(_value, "isoformat"):
       |            _values.append(int(_value.value))
       |        elif hasattr(_value, "item"):
       |            _values.append(_value.item())
       |        else:
       |            _values.append(_value)
       |    _cells[_column] = _values
       |_cells["size_sum"] = float(out1df["size"].sum())
       |print("JSON " + json.dumps(_cells))""".stripMargin

  /** The python the runtime test runs, if one on this machine can read Parquet. */
  private def runnablePython(): Option[String] = {
    val fromConfig = Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
      .orElse(Try(ConfigFactory.load()).toOption)
      .flatMap(config => Try(config.getConfig("python").getString("path")).toOption)
      .map(_.trim)
      .filter(_.nonEmpty)

    def reads(executable: String): Boolean =
      Try(
        new ProcessBuilder(executable, "-c", "import pandas, pyarrow")
          .redirectErrorStream(true)
          .start()
      ).toOption.exists { process =>
        if (!process.waitFor(60, TimeUnit.SECONDS)) { process.destroyForcibly(); false }
        else process.exitValue() == 0
      }

    (fromConfig.toList ++ List("python3", "python")).distinct.find(reads)
  }
}
