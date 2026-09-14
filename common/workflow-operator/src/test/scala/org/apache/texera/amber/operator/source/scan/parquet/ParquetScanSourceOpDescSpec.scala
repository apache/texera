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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Types}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import scala.jdk.CollectionConverters._
import scala.util.Using

class ParquetScanSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  /** A one-row Parquet file over the four types the mapping treats specially. */
  private def writeSampleFile(): File = {
    val file = File.createTempFile("parquet-src-", ".parquet")
    file.delete() // the writer refuses to overwrite
    file.deleteOnExit()
    val messageType: MessageType = Types
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
      val filled: Group = factory.newGroup()
      filled.append("id", 7)
      filled.append("name", "alice")
      filled.append("score", 1.5d)
      filled.append(
        "seen_at",
        LocalDateTime.of(2024, 3, 2, 14, 5, 9).toInstant(ZoneOffset.UTC).toEpochMilli
      )
      filled.append("active", true)
      writer.write(filled)
      // Every column optional, so a second row can leave them all out. Parquet
      // has no null value: a field that repeats zero times is the hole.
      writer.write(factory.newGroup())
    }.get
    file
  }

  /** A one-row file over a single timestamp column, counted in `unit`. */
  private def writeTimestampFile(unit: LogicalTypeAnnotation.TimeUnit, count: Long): File = {
    val file = File.createTempFile("parquet-ts-", ".parquet")
    file.delete()
    file.deleteOnExit()
    val messageType: MessageType = Types
      .buildMessage()
      .addField(
        Types
          .optional(PrimitiveTypeName.INT64)
          .as(LogicalTypeAnnotation.timestampType(true, unit))
          .named("seen_at")
      )
      .named("sample")

    val conf = new Configuration()
    GroupWriteSupport.setSchema(messageType, conf)
    Using(
      ExampleParquetWriter
        .builder(new HadoopPath(file.getAbsolutePath))
        .withConf(conf)
        .withType(messageType)
        .build()
    )(_.write(new SimpleGroupFactory(messageType).newGroup().append("seen_at", count))).get
    file
  }

  /**
    * A one-row file over the same decimal written in each storage Parquet allows
    * for one: the integer 1234 with a scale of 2, which is 12.34.
    */
  private def writeDecimalFile(): File = {
    val file = File.createTempFile("parquet-dec-", ".parquet")
    file.delete()
    file.deleteOnExit()
    val decimal = LogicalTypeAnnotation.decimalType(2, 9)
    val messageType: MessageType = Types
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

    val conf = new Configuration()
    GroupWriteSupport.setSchema(messageType, conf)
    Using(
      ExampleParquetWriter
        .builder(new HadoopPath(file.getAbsolutePath))
        .withConf(conf)
        .withType(messageType)
        .build()
    ) { writer =>
      writer.write(
        new SimpleGroupFactory(messageType)
          .newGroup()
          .append("in_int", 1234)
          .append("in_long", 1234L)
          // The wide storages hold the integer as its big-endian two's complement.
          .append("in_bytes", Binary.fromConstantByteArray(Array[Byte](0, 0, 4, -46)))
      )
    }.get
    file
  }

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
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some(writeSampleFile().toURI.toString)
    val schema = d.inferSchema()
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
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some(writeDecimalFile().toURI.toString)
    d.inferSchema().getAttributes.map(_.getType) shouldBe
      List(AttributeType.DOUBLE, AttributeType.DOUBLE, AttributeType.DOUBLE)

    val exec = new ParquetScanSourceOpExec(objectMapper.writeValueAsString(d))
    exec.open()
    try {
      exec
        .produceTuple()
        .next()
        .asInstanceOf[org.apache.texera.amber.core.tuple.SeqTupleLike]
        .getFields
        .toList shouldBe List(12.34d, 12.34d, 12.34d)
    } finally exec.close()
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

  "ParquetScanSourceOpDesc.getPhysicalOp" should
    "wire the Parquet exec as a source op with no input port and one output port" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some(writeSampleFile().toURI.toString)
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
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some(writeSampleFile().toURI.toString)
    val exec = new ParquetScanSourceOpExec(objectMapper.writeValueAsString(d))
    exec.open()
    try {
      val rows = exec.produceTuple().toList
      rows should have length 2
      val first = rows.head.asInstanceOf[org.apache.texera.amber.core.tuple.SeqTupleLike]
      first.getFields.toList shouldBe List(
        7,
        "alice",
        1.5d,
        Timestamp.valueOf(LocalDateTime.of(2024, 3, 2, 14, 5, 9)),
        true
      )
      // The second row wrote no field at all, so every cell is a hole.
      rows(1)
        .asInstanceOf[org.apache.texera.amber.core.tuple.SeqTupleLike]
        .getFields
        .toList shouldBe List(null, null, null, null, null)
    } finally exec.close()
  }

  // The timestamp is the case a zone could quietly enter. The file counts from
  // the epoch; a Texera TIMESTAMP is a wall clock. Reading it with the machine's
  // own zone would move it, and the exported script would not agree.
  it should "read a timestamp as the wall clock the file counts to, in any zone" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some(writeSampleFile().toURI.toString)
    val exec = new ParquetScanSourceOpExec(objectMapper.writeValueAsString(d))
    exec.open()
    try {
      val seenAt = exec
        .produceTuple()
        .next()
        .asInstanceOf[org.apache.texera.amber.core.tuple.SeqTupleLike]
        .getFields(3)
      seenAt shouldBe Timestamp.valueOf("2024-03-02 14:05:09")
    } finally exec.close()
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
        val d = new ParquetScanSourceOpDesc
        d.fileName = Some(writeTimestampFile(unit, count).toURI.toString)
        val exec = new ParquetScanSourceOpExec(objectMapper.writeValueAsString(d))
        exec.open()
        try {
          exec
            .produceTuple()
            .next()
            .asInstanceOf[org.apache.texera.amber.core.tuple.SeqTupleLike]
            .getFields
            .head shouldBe Timestamp.valueOf(wallClock)
        } finally exec.close()
    }
  }

  "ParquetScanSourceOpDesc.generateStandaloneCode" should "read the file by its own name" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some("file:///tmp/some%20dir/data.parquet")
    d.generateStandaloneCode() should startWith("""out1df = pd.read_parquet("data.parquet")""")
  }

  // pandas fills a DECIMAL column with decimal.Decimal objects, which do not mix
  // with the floats the executor reads the same column as.
  it should "cast the columns pandas reads as decimals" in {
    val d = new ParquetScanSourceOpDesc
    d.fileName = Some("file:///tmp/data.parquet")
    d.standaloneImports() should contain("from decimal import Decimal")
    d.generateStandaloneCode() should include("_values.astype(float)")
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
}
