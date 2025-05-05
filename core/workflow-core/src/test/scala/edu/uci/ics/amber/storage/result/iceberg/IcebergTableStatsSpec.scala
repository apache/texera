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

package edu.uci.ics.amber.storage.result.iceberg

import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSURIFactory}
import edu.uci.ics.amber.util.IcebergUtil
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.Record
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.net.URI
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.UUID

class IcebergTableStatsSpec extends AnyFlatSpec with BeforeAndAfterAll with Suite {

  var amberSchema: Schema = _
  var icebergSchema: IcebergSchema = _
  var serde: (IcebergSchema, Tuple) => Record = _
  var deserde: (IcebergSchema, Record) => Tuple = _
  var catalog: Catalog = _
  val tableNamespace = "test_namespace"
  var uri: URI = VFSURIFactory.createResultURI(
    WorkflowIdentity(0),
    ExecutionIdentity(0),
    GlobalPortIdentity(
      PhysicalOpIdentity(
        logicalOpId =
          OperatorIdentity(s"test_table_${UUID.randomUUID().toString.replace("-", "")}"),
        layerName = "main"
      ),
      PortIdentity()
    )
  )

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Initialize Amber Schema with all possible attribute types
    amberSchema = Schema(
      List(
        new Attribute("col-string", AttributeType.STRING),
        new Attribute("col-int", AttributeType.INTEGER),
        new Attribute("col-bool", AttributeType.BOOLEAN),
        new Attribute("col-long", AttributeType.LONG),
        new Attribute("col-double", AttributeType.DOUBLE),
        new Attribute("col-timestamp", AttributeType.TIMESTAMP),
        new Attribute("col-binary", AttributeType.BINARY)
      )
    )

    // Initialize Iceberg Schema
    icebergSchema = IcebergUtil.toIcebergSchema(amberSchema)

    // Initialize serialization and deserialization functions
    serde = IcebergUtil.toGenericRecord
    deserde = (schema, record) => IcebergUtil.fromRecord(record, amberSchema)
  }

  behavior of "TableStatistics"

  it should "get correct statistics after inserting three tuples" in {
    DocumentFactory.createDocument(uri, amberSchema)
    val document = DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]]

    val tuples = generateSampleItems(
      List(
        (
          "first",
          Some(42),
          Some(true),
          Some(100L),
          Some(3.14),
          Some(10000L),
          Some(List(ByteBuffer.wrap(Array[Byte](1, 2, 3))))
        ),
        (
          "second",
          Some(50),
          Some(false),
          Some(200L),
          Some(2.71),
          Some(20000L),
          Some(List(ByteBuffer.wrap(Array[Byte](4, 5, 6))))
        ),
        (
          "third",
          Some(30),
          Some(true),
          Some(150L),
          Some(1.41),
          Some(15000L),
          Some(List(ByteBuffer.wrap(Array[Byte](7, 8, 9))))
        )
      )
    )

    // Get writer and write items
    val writer = document.writer(UUID.randomUUID().toString)
    writer.open()
    tuples.foreach(writer.putOne)
    writer.close()

    val stats = document.getTableStatistics

    assert(!stats("col-string").contains("min"))
    assert(!stats("col-string").contains("max"))
    assert(stats("col-int")("min") == 30)
    assert(stats("col-int")("max") == 50)
    assert(stats("col-long")("min") == 100L)
    assert(stats("col-long")("max") == 200L)
    assert(stats("col-double")("min") == 1.41)
    assert(stats("col-double")("max") == 3.14)
    val formatter = DateTimeFormatter.ISO_LOCAL_DATE
    val actualMin = LocalDate.parse(stats("col-timestamp")("min").asInstanceOf[String], formatter)
    val actualMax = LocalDate.parse(stats("col-timestamp")("max").asInstanceOf[String], formatter)
    val expectedMin = new Timestamp(10000L).toInstant.atZone(ZoneId.systemDefault()).toLocalDate
    val expectedMax = new Timestamp(20000L).toInstant.atZone(ZoneId.systemDefault()).toLocalDate
    assert(actualMin == expectedMin)
    assert(actualMax == expectedMax)
    assert(stats("col-bool")("not_null_count") == 3L)
  }

  it should "get updated statistics after adding new tuples" in {
    val newTuples = generateSampleItems(
      List(
        (
          "new-first",
          Some(99),
          Some(false),
          Some(300L),
          Some(4.56),
          Some(30000L),
          Some(List(ByteBuffer.wrap(Array[Byte](10, 11, 12))))
        ),
        (
          "new-second",
          Some(77),
          Some(true),
          Some(400L),
          Some(5.67),
          Some(40000L),
          Some(List(ByteBuffer.wrap(Array[Byte](13, 14, 15))))
        )
      )
    )
    val document = DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]]

    // Get writer and write items
    val writer = document.writer(UUID.randomUUID().toString)
    writer.open()
    newTuples.foreach(writer.putOne)
    writer.close()

    val stats = document.getTableStatistics

    assert(stats("col-int")("min") == 30)
    assert(stats("col-int")("max") == 99)
    assert(stats("col-long")("min") == 100L)
    assert(stats("col-long")("max") == 400L)
    assert(stats("col-double")("min") == 1.41)
    assert(stats("col-double")("max") == 5.67)
    val formatter = DateTimeFormatter.ISO_LOCAL_DATE
    val actualMin = LocalDate.parse(stats("col-timestamp")("min").asInstanceOf[String], formatter)
    val actualMax = LocalDate.parse(stats("col-timestamp")("max").asInstanceOf[String], formatter)
    val expectedMin = new Timestamp(10000L).toInstant.atZone(ZoneId.systemDefault()).toLocalDate
    val expectedMax = new Timestamp(40000L).toInstant.atZone(ZoneId.systemDefault()).toLocalDate
    assert(actualMin == expectedMin)
    assert(actualMax == expectedMax)
    assert(stats("col-bool")("not_null_count") == 5L)
  }

  it should "correctly count non-null values in the presence of null values" in {
    val tuplesWithNulls = generateSampleItems(
      List(
        (
          "first",
          Some(42),
          None,
          Some(100L),
          Some(3.14),
          Some(10000L),
          Some(List(ByteBuffer.wrap(Array[Byte](1, 2, 3))))
        ),
        (
          "second",
          None,
          Some(false),
          Some(200L),
          Some(2.71),
          Some(20000L),
          Some(List(ByteBuffer.wrap(Array[Byte](4, 5, 6))))
        ),
        ("third", Some(30), Some(true), None, Some(1.41), Some(15000L), None)
      )
    )
    val document = DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]]

    val writer = document.writer(UUID.randomUUID().toString)
    writer.open()
    tuplesWithNulls.foreach(writer.putOne)
    writer.close()

    val stats = document.getTableStatistics

    assert(stats("col-string")("not_null_count") == 8L)
    assert(stats("col-int")("not_null_count") == 7L)
    assert(stats("col-bool")("not_null_count") == 7L)
    assert(stats("col-long")("not_null_count") == 7L)
    assert(stats("col-double")("not_null_count") == 8L)
    assert(stats("col-timestamp")("not_null_count") == 8L)
    assert(stats("col-binary")("not_null_count") == 8L)
  }

  def generateSampleItems(
      values: List[
        (
            String,
            Option[Int],
            Option[Boolean],
            Option[Long],
            Option[Double],
            Option[Long],
            Option[List[ByteBuffer]]
        )
      ]
  ): List[Tuple] = {
    values.map {
      case (strVal, intVal, boolVal, longVal, doubleVal, timestampVal, binaryVal) =>
        Tuple
          .builder(amberSchema)
          .add("col-string", AttributeType.STRING, strVal)
          .add("col-int", AttributeType.INTEGER, intVal.orNull)
          .add("col-bool", AttributeType.BOOLEAN, boolVal.orNull)
          .add("col-long", AttributeType.LONG, longVal.orNull)
          .add("col-double", AttributeType.DOUBLE, doubleVal.orNull)
          .add("col-timestamp", AttributeType.TIMESTAMP, timestampVal.map(new Timestamp(_)).orNull)
          .add("col-binary", AttributeType.BINARY, binaryVal.orNull)
          .build()
    }
  }
}
