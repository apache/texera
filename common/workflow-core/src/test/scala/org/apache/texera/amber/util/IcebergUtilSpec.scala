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

package org.apache.texera.amber.util

import org.apache.texera.amber.core.tuple.{AttributeType, LargeBinary, Schema, Tuple}
import org.apache.texera.amber.util.IcebergUtil.toIcebergSchema
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.types.Types
import org.apache.iceberg.{Schema => IcebergSchema}
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}
import scala.jdk.CollectionConverters._

class IcebergUtilSpec extends AnyFlatSpec {

  val texeraSchema: Schema = Schema()
    .add("test-1", AttributeType.INTEGER)
    .add("test-2", AttributeType.LONG)
    .add("test-3", AttributeType.BOOLEAN)
    .add("test-4", AttributeType.DOUBLE)
    .add("test-5", AttributeType.TIMESTAMP)
    .add("test-6", AttributeType.STRING)
    .add("test-7", AttributeType.BINARY)

  val icebergSchema: IcebergSchema = new IcebergSchema(
    List(
      Types.NestedField.optional(1, "test-1", Types.IntegerType.get()),
      Types.NestedField.optional(2, "test-2", Types.LongType.get()),
      Types.NestedField.optional(3, "test-3", Types.BooleanType.get()),
      Types.NestedField.optional(4, "test-4", Types.DoubleType.get()),
      Types.NestedField.optional(5, "test-5", Types.TimestampType.withoutZone()),
      Types.NestedField.optional(6, "test-6", Types.StringType.get()),
      Types.NestedField.optional(7, "test-7", Types.BinaryType.get())
    ).asJava
  )

  behavior of "IcebergUtil"

  it should "convert from AttributeType to Iceberg Type correctly" in {
    assert(IcebergUtil.toIcebergType(AttributeType.INTEGER) == Types.IntegerType.get())
    assert(IcebergUtil.toIcebergType(AttributeType.LONG) == Types.LongType.get())
    assert(IcebergUtil.toIcebergType(AttributeType.BOOLEAN) == Types.BooleanType.get())
    assert(IcebergUtil.toIcebergType(AttributeType.DOUBLE) == Types.DoubleType.get())
    assert(IcebergUtil.toIcebergType(AttributeType.TIMESTAMP) == Types.TimestampType.withoutZone())
    assert(IcebergUtil.toIcebergType(AttributeType.STRING) == Types.StringType.get())
    assert(IcebergUtil.toIcebergType(AttributeType.BINARY) == Types.BinaryType.get())
  }

  it should "convert from Iceberg Type to AttributeType correctly" in {
    assert(IcebergUtil.fromIcebergType(Types.IntegerType.get()) == AttributeType.INTEGER)
    assert(IcebergUtil.fromIcebergType(Types.LongType.get()) == AttributeType.LONG)
    assert(IcebergUtil.fromIcebergType(Types.BooleanType.get()) == AttributeType.BOOLEAN)
    assert(IcebergUtil.fromIcebergType(Types.DoubleType.get()) == AttributeType.DOUBLE)
    assert(
      IcebergUtil.fromIcebergType(Types.TimestampType.withoutZone()) == AttributeType.TIMESTAMP
    )
    assert(IcebergUtil.fromIcebergType(Types.StringType.get()) == AttributeType.STRING)
    assert(IcebergUtil.fromIcebergType(Types.BinaryType.get()) == AttributeType.BINARY)
  }

  it should "convert from Texera Schema to Iceberg Schema correctly" in {
    assert(IcebergUtil.toIcebergSchema(texeraSchema).sameSchema(icebergSchema))
  }

  it should "convert from Iceberg Schema to Texera Schema correctly" in {
    assert(IcebergUtil.fromIcebergSchema(icebergSchema) == texeraSchema)
  }

  it should "convert Texera Tuple to Iceberg GenericRecord correctly" in {
    val tuple = Tuple
      .builder(texeraSchema)
      .addSequentially(
        Array(
          Int.box(42),
          Long.box(123456789L),
          Boolean.box(true),
          Double.box(3.14),
          new Timestamp(10000L),
          "hello world",
          Array[Byte](1, 2, 3, 4)
        )
      )
      .build()

    val record = IcebergUtil.toGenericRecord(toIcebergSchema(tuple.schema), tuple)

    assert(record.getField("test-1") == 42)
    assert(record.getField("test-2") == 123456789L)
    assert(record.getField("test-3") == true)
    assert(record.getField("test-4") == 3.14)
    assert(record.getField("test-5") == new Timestamp(10000L).toLocalDateTime)
    assert(record.getField("test-6") == "hello world")
    assert(record.getField("test-7") == ByteBuffer.wrap(Array[Byte](1, 2, 3, 4)))

    val tupleFromRecord = IcebergUtil.fromRecord(record, texeraSchema)
    assert(tupleFromRecord == tuple)
  }

  it should "convert Texera Tuple with null values to Iceberg GenericRecord correctly" in {
    val tuple = Tuple
      .builder(texeraSchema)
      .addSequentially(
        Array(
          Int.box(42), // Non-null
          null, // Null Long
          Boolean.box(true), // Non-null
          null, // Null Double
          null, // Null Timestamp
          "hello world", // Non-null String
          null // Null Binary
        )
      )
      .build()

    val record = IcebergUtil.toGenericRecord(toIcebergSchema(tuple.schema), tuple)

    assert(record.getField("test-1") == 42)
    assert(record.getField("test-2") == null)
    assert(record.getField("test-3") == true)
    assert(record.getField("test-4") == null)
    assert(record.getField("test-5") == null)
    assert(record.getField("test-6") == "hello world")
    assert(record.getField("test-7") == null)

    val tupleFromRecord = IcebergUtil.fromRecord(record, texeraSchema)
    assert(tupleFromRecord == tuple)
  }

  it should "convert a fully null Texera Tuple to Iceberg GenericRecord correctly" in {
    val tuple = Tuple
      .builder(texeraSchema)
      .addSequentially(
        Array(
          null, // Null Integer
          null, // Null Long
          null, // Null Boolean
          null, // Null Double
          null, // Null Timestamp
          null, // Null String
          null // Null Binary
        )
      )
      .build()

    val record = IcebergUtil.toGenericRecord(toIcebergSchema(tuple.schema), tuple)

    assert(record.getField("test-1") == null)
    assert(record.getField("test-2") == null)
    assert(record.getField("test-3") == null)
    assert(record.getField("test-4") == null)
    assert(record.getField("test-5") == null)
    assert(record.getField("test-6") == null)
    assert(record.getField("test-7") == null)

    val tupleFromRecord = IcebergUtil.fromRecord(record, texeraSchema)
    assert(tupleFromRecord == tuple)
  }

  it should "convert Iceberg GenericRecord to Texera Tuple correctly" in {
    val record = GenericRecord.create(icebergSchema)
    record.setField("test-1", 42)
    record.setField("test-2", 123456789L)
    record.setField("test-3", true)
    record.setField("test-4", 3.14)
    record.setField(
      "test-5",
      LocalDateTime.ofInstant(new Timestamp(10000L).toInstant, ZoneId.systemDefault())
    )
    record.setField("test-6", "hello world")
    record.setField("test-7", ByteBuffer.wrap(Array[Byte](1, 2, 3, 4)))

    val tuple = IcebergUtil.fromRecord(record, texeraSchema)

    assert(tuple.getField[Integer]("test-1") == 42)
    assert(tuple.getField[Long]("test-2") == 123456789L)
    assert(tuple.getField[Boolean]("test-3") == true)
    assert(tuple.getField[Double]("test-4") == 3.14)
    assert(tuple.getField[Timestamp]("test-5") == new Timestamp(10000L))
    assert(tuple.getField[String]("test-6") == "hello world")
    assert(tuple.getField[Array[Byte]]("test-7") sameElements Array[Byte](1, 2, 3, 4))
  }

  // LARGE_BINARY type tests

  it should "convert LARGE_BINARY type correctly between Texera and Iceberg" in {
    // LARGE_BINARY stored as StringType with field name suffix
    assert(IcebergUtil.toIcebergType(AttributeType.LARGE_BINARY) == Types.StringType.get())
    assert(IcebergUtil.fromIcebergType(Types.StringType.get(), "field") == AttributeType.STRING)
    assert(
      IcebergUtil.fromIcebergType(
        Types.StringType.get(),
        "field__texera_large_binary_ptr"
      ) == AttributeType.LARGE_BINARY
    )
  }

  it should "convert schemas with LARGE_BINARY fields correctly" in {
    val texeraSchema = Schema()
      .add("id", AttributeType.INTEGER)
      .add("large_data", AttributeType.LARGE_BINARY)

    val icebergSchema = IcebergUtil.toIcebergSchema(texeraSchema)

    // LARGE_BINARY field gets encoded name with suffix
    assert(icebergSchema.findField("large_data__texera_large_binary_ptr") != null)
    assert(
      icebergSchema.findField("large_data__texera_large_binary_ptr").`type`() == Types.StringType
        .get()
    )

    // Round-trip preserves schema
    val roundTripSchema = IcebergUtil.fromIcebergSchema(icebergSchema)
    assert(roundTripSchema.getAttribute("large_data").getType == AttributeType.LARGE_BINARY)
  }

  it should "convert tuples with LARGE_BINARY to records and back correctly" in {
    val schema = Schema()
      .add("id", AttributeType.INTEGER)
      .add("large_data", AttributeType.LARGE_BINARY)

    val tuple = Tuple
      .builder(schema)
      .addSequentially(Array(Int.box(42), new LargeBinary("s3://bucket/object/key.data")))
      .build()

    val record = IcebergUtil.toGenericRecord(toIcebergSchema(schema), tuple)

    // LARGE_BINARY stored as URI string with encoded field name
    assert(record.getField("id") == 42)
    assert(record.getField("large_data__texera_large_binary_ptr") == "s3://bucket/object/key.data")

    // Round-trip preserves data
    val roundTripTuple = IcebergUtil.fromRecord(record, schema)
    assert(roundTripTuple == tuple)

    // LargeBinary properties are accessible
    val largeBinary = roundTripTuple.getField[LargeBinary]("large_data")
    assert(largeBinary.getUri == "s3://bucket/object/key.data")
    assert(largeBinary.getBucketName == "bucket")
    assert(largeBinary.getObjectKey == "object/key.data")
  }

  it should "handle null LARGE_BINARY values correctly" in {
    val schema = Schema().add("data", AttributeType.LARGE_BINARY)

    val tupleWithNull = Tuple.builder(schema).addSequentially(Array(null)).build()
    val record = IcebergUtil.toGenericRecord(toIcebergSchema(schema), tupleWithNull)

    assert(record.getField("data__texera_large_binary_ptr") == null)
    assert(IcebergUtil.fromRecord(record, schema) == tupleWithNull)
  }

  it should "handle multiple LARGE_BINARY fields and mixed types correctly" in {
    val schema = Schema()
      .add("int_field", AttributeType.INTEGER)
      .add("large_binary_1", AttributeType.LARGE_BINARY)
      .add("string_field", AttributeType.STRING)
      .add("large_binary_2", AttributeType.LARGE_BINARY)

    val tuple = Tuple
      .builder(schema)
      .addSequentially(
        Array(
          Int.box(123),
          new LargeBinary("s3://bucket1/file1.dat"),
          "normal string",
          null // null LARGE_BINARY
        )
      )
      .build()

    val record = IcebergUtil.toGenericRecord(toIcebergSchema(schema), tuple)

    assert(record.getField("int_field") == 123)
    assert(record.getField("large_binary_1__texera_large_binary_ptr") == "s3://bucket1/file1.dat")
    assert(record.getField("string_field") == "normal string")
    assert(record.getField("large_binary_2__texera_large_binary_ptr") == null)

    assert(IcebergUtil.fromRecord(record, schema) == tuple)
  }
}
