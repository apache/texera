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

package org.apache.texera.amber.core.storage.result.iceberg

import org.apache.texera.amber.core.storage.IcebergCatalogInstance
import org.apache.texera.amber.core.storage.LocalInMemoryIcebergCatalog
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.IcebergUtil
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.Record
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.{Schema => IcebergSchema}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.UUID
import java.util.zip.ZipInputStream

/**
  * Unit-level tests for [[IcebergDocument]] running against a local in-memory
  * Iceberg catalog (temp `file:/` warehouse) installed into the shared
  * `IcebergCatalogInstance` singleton via [[LocalInMemoryIcebergCatalog]].
  *
  * `IcebergDocument` reads its catalog from `IcebergCatalogInstance.getInstance()`,
  * so the catalog must be installed before any document access. Each test creates a
  * fresh, uniquely-named table so the read/write/count/clear paths are isolated.
  */
class IcebergDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val tableNamespace = "iceberg_doc_spec"

  private val amberSchema: Schema = Schema()
    .add("id", AttributeType.INTEGER)
    .add("amount", AttributeType.LONG)
    .add("score", AttributeType.DOUBLE)
    .add("name", AttributeType.STRING)
    .add("ts", AttributeType.TIMESTAMP)

  private val icebergSchema: IcebergSchema = IcebergUtil.toIcebergSchema(amberSchema)

  private val serde: (IcebergSchema, Tuple) => Record = IcebergUtil.toGenericRecord
  private val deserde: (IcebergSchema, Record) => Tuple =
    (schema, record) => IcebergUtil.fromRecord(record, IcebergUtil.fromIcebergSchema(schema))

  override def beforeAll(): Unit = {
    super.beforeAll()
    LocalInMemoryIcebergCatalog.ensure()
  }

  private def freshTableName(): String =
    s"tbl_${UUID.randomUUID().toString.replace("-", "")}"

  /** Create the backing table and return a document handle for it. */
  private def newDocument(tableName: String = freshTableName()): IcebergDocument[Tuple] = {
    IcebergUtil.createTable(
      IcebergCatalogInstance.getInstance(),
      tableNamespace,
      tableName,
      icebergSchema,
      overrideIfExists = true
    )
    new IcebergDocument[Tuple](tableNamespace, tableName, icebergSchema, serde, deserde)
  }

  private def tuple(id: Int): Tuple =
    Tuple
      .builder(amberSchema)
      .add("id", AttributeType.INTEGER, Int.box(id))
      .add("amount", AttributeType.LONG, Long.box(id.toLong * 100L))
      .add("score", AttributeType.DOUBLE, Double.box(id.toDouble + 0.5))
      .add("name", AttributeType.STRING, s"name-$id")
      .add("ts", AttributeType.TIMESTAMP, new Timestamp(1_600_000_000_000L + id))
      .build()

  /** Write the given tuples through a single writer session (one committed file). */
  private def write(doc: IcebergDocument[Tuple], tuples: Seq[Tuple]): Unit = {
    val writer = doc.writer(UUID.randomUUID().toString)
    writer.open()
    tuples.foreach(writer.putOne)
    writer.close()
  }

  "IcebergDocument" should "resolve the table location through getURI for an existing table" in {
    val doc = newDocument()
    // getURI loads the table metadata and wraps table.location() in URI.create.
    // On a local `file:/` warehouse the location string carries the table name.
    // On Windows the warehouse resolves to a raw `C:\...` path that URI.create
    // rejects; in that case the method still executes its whole body (metadata
    // load + location() + URI.create) before throwing, so it still pins the
    // existing-table branch. The IllegalArgumentException escape is therefore
    // allowed ONLY on Windows — elsewhere getURI must return a valid URI.
    val isWindows = System.getProperty("os.name").toLowerCase.contains("win")
    try {
      doc.getURI.toString should include(doc.tableName)
    } catch {
      case _: IllegalArgumentException if isWindows => succeed
    }
  }

  it should "throw NoSuchTableException from getURI when the table does not exist" in {
    val doc = new IcebergDocument[Tuple](
      tableNamespace,
      freshTableName(),
      icebergSchema,
      serde,
      deserde
    )
    intercept[NoSuchTableException] {
      doc.getURI
    }
  }

  it should "return count 0 for a freshly created, empty table" in {
    val doc = newDocument()
    doc.getCount shouldBe 0L
    doc.get().hasNext shouldBe false
  }

  it should "return count 0 for a table that was never created" in {
    val doc = new IcebergDocument[Tuple](
      tableNamespace,
      freshTableName(),
      icebergSchema,
      serde,
      deserde
    )
    doc.getCount shouldBe 0L
  }

  it should "count and read back all written records" in {
    val doc = newDocument()
    val tuples = (0 until 10).map(tuple)
    write(doc, tuples)

    doc.getCount shouldBe 10L
    val read = doc.get().toList
    read should have size 10
    read.map(_.getField[Int]("id")).toSet shouldBe (0 until 10).toSet
  }

  it should "read records in file-sequence order within a single committed file" in {
    val doc = newDocument()
    val tuples = (0 until 6).map(tuple)
    write(doc, tuples)

    doc.get().toList.map(_.getField[Int]("id")) shouldBe (0 until 6).toList
  }

  it should "return only the requested range via getRange" in {
    val doc = newDocument()
    write(doc, (0 until 10).map(tuple))

    doc.getRange(2, 5).toList.map(_.getField[Int]("id")) shouldBe List(2, 3, 4)
  }

  it should "return records from an offset (inclusive) via getAfter" in {
    val doc = newDocument()
    write(doc, (0 until 5).map(tuple))

    doc.getAfter(3).toList.map(_.getField[Int]("id")) shouldBe List(3, 4)
  }

  it should "project only the requested columns via getRange" in {
    val doc = newDocument()
    write(doc, (0 until 4).map(tuple))

    val projected = doc.getRange(0, 4, Some(Seq("id"))).toList
    projected.map(_.getField[Int]("id")) shouldBe List(0, 1, 2, 3)
  }

  it should "read incrementally across multiple committed files" in {
    val doc = newDocument()
    write(doc, (0 until 5).map(tuple))
    write(doc, (5 until 10).map(tuple))

    doc.getCount shouldBe 10L
    doc.get().toList.map(_.getField[Int]("id")).toSet shouldBe (0 until 10).toSet
  }

  it should "throw NoSuchElementException when advancing an exhausted iterator" in {
    val doc = newDocument()
    write(doc, Seq(tuple(1)))
    val it = doc.get()
    it.next()
    it.hasNext shouldBe false
    intercept[NoSuchElementException] {
      it.next()
    }
  }

  it should "compute per-field statistics for numeric, string and timestamp columns" in {
    val doc = newDocument()
    write(doc, (1 to 5).map(tuple))

    val stats = doc.getTableStatistics
    stats.keySet should contain allOf ("id", "amount", "score", "name", "ts")

    // Numeric fields expose min/max plus a not-null count.
    stats("id")("min").asInstanceOf[Double] shouldBe 1.0
    stats("id")("max").asInstanceOf[Double] shouldBe 5.0
    stats("id")("not_null_count").asInstanceOf[Long] shouldBe 5L
    stats("amount")("min").asInstanceOf[Double] shouldBe 100.0
    stats("amount")("max").asInstanceOf[Double] shouldBe 500.0
    stats("score")("max").asInstanceOf[Double] shouldBe 5.5

    // String field only carries the not-null count (no min/max keys).
    stats("name")("not_null_count").asInstanceOf[Long] shouldBe 5L
    stats("name").contains("min") shouldBe false

    // Timestamp field exposes min/max as ISO local-date strings.
    stats("ts") should contain key "min"
    stats("ts") should contain key "max"
  }

  it should "throw NoSuchTableException from getTableStatistics when the table is missing" in {
    val doc = new IcebergDocument[Tuple](
      tableNamespace,
      freshTableName(),
      icebergSchema,
      serde,
      deserde
    )
    intercept[NoSuchTableException] {
      doc.getTableStatistics
    }
  }

  it should "report a positive total file size after writes and throw when missing" in {
    val doc = newDocument()
    write(doc, (0 until 8).map(tuple))
    doc.getTotalFileSize should be > 0L

    val missing = new IcebergDocument[Tuple](
      tableNamespace,
      freshTableName(),
      icebergSchema,
      serde,
      deserde
    )
    intercept[NoSuchTableException] {
      missing.getTotalFileSize
    }
  }

  it should "expose an empty stream for an empty table via asInputStream" in {
    val doc = newDocument()
    val stream = doc.asInputStream()
    try {
      stream.readAllBytes() shouldBe empty
    } finally {
      stream.close()
    }
  }

  it should "expose written rows as a non-empty ZIP archive via asInputStream" in {
    val doc = newDocument()
    write(doc, (0 until 3).map(tuple))

    val stream = doc.asInputStream()
    try {
      val bytes = stream.readAllBytes()
      bytes.length should be > 0
      // ZIP local-file-header magic bytes: 0x50 0x4B ("PK").
      bytes(0) shouldBe 0x50.toByte
      bytes(1) shouldBe 0x4b.toByte
    } finally {
      stream.close()
    }

    // The archive should carry at least one .parquet entry.
    val zip = new ZipInputStream(doc.asInputStream())
    try {
      val entry = zip.getNextEntry
      entry should not be null
      entry.getName should endWith(".parquet")
    } finally {
      zip.close()
    }
  }

  it should "drop the table on clear so it no longer exists and counts as empty" in {
    val doc = newDocument()
    write(doc, (0 until 4).map(tuple))
    doc.getCount shouldBe 4L

    doc.clear()

    IcebergCatalogInstance
      .getInstance()
      .tableExists(TableIdentifier.of(tableNamespace, doc.tableName)) shouldBe false
    doc.getCount shouldBe 0L
  }

  it should "be a no-op when clearing a table that does not exist" in {
    val doc = new IcebergDocument[Tuple](
      tableNamespace,
      freshTableName(),
      icebergSchema,
      serde,
      deserde
    )
    noException should be thrownBy doc.clear()
  }

  it should "expose the constructed table identity and behave as a VirtualDocument" in {
    val name = freshTableName()
    val doc: VirtualDocument[Tuple] = newDocument(name)
    doc.asInstanceOf[IcebergDocument[Tuple]].tableNamespace shouldBe tableNamespace
    doc.asInstanceOf[IcebergDocument[Tuple]].tableName shouldBe name
  }
}
