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

import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.storage.IcebergCatalogInstance
import org.apache.texera.amber.util.IcebergUtil
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.{Schema => IcebergSchema, Table}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.{Files, Path}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

class IcebergTableWriterSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private val tableNamespace = "writer_spec"
  private var warehouseDir: Path = _
  private var catalog: Catalog = _

  private val amberSchema: Schema = Schema()
    .add("id", AttributeType.INTEGER)
    .add("name", AttributeType.STRING)

  private val icebergSchema: IcebergSchema = IcebergUtil.toIcebergSchema(amberSchema)

  // The writer resolves its catalog from the shared cache per use (#7290), so the
  // spec's local catalog is registered under a spec-unique warehouse name.
  private val specWarehouse = "iceberg-table-writer-spec"

  override def beforeAll(): Unit = {
    warehouseDir = Files.createTempDirectory("iceberg-table-writer-spec")
    catalog = IcebergUtil.createHadoopCatalog("writer-spec", warehouseDir)
    IcebergCatalogInstance.replaceInstance(catalog, Some(specWarehouse))
  }

  override def afterAll(): Unit = {
    catalog match {
      case closeable: AutoCloseable => closeable.close()
      case _                        =>
    }
  }

  private def tuple(id: Int): Tuple =
    Tuple
      .builder(amberSchema)
      .addSequentially(Array(Int.box(id), s"name-$id"))
      .build()

  private def createWriter(tableName: String, writerIdentifier: String = "writer_0") = {
    IcebergUtil.createTable(
      catalog,
      tableNamespace,
      tableName,
      icebergSchema,
      overrideIfExists = true
    )
    new IcebergTableWriter[Tuple](
      writerIdentifier,
      Some(specWarehouse),
      tableNamespace,
      tableName,
      icebergSchema,
      IcebergUtil.toGenericRecord
    )
  }

  private def loadTable(tableName: String): Table =
    IcebergUtil.loadTableMetadata(catalog, tableNamespace, tableName).get

  private def readTuples(tableName: String): List[Tuple] = {
    val records = IcebergGenerics.read(loadTable(tableName)).build()
    try {
      records.iterator().asScala.map(IcebergUtil.fromRecord(_, amberSchema)).toList
    } finally {
      records.close()
    }
  }

  private def freshTableName(): String =
    s"table_${UUID.randomUUID().toString.replace("-", "")}"

  "IcebergTableWriter" should "flush remaining buffered tuples to the table on close" in {
    val tableName = freshTableName()
    val writer = createWriter(tableName)
    writer.open()
    val tuples = (0 until 10).map(tuple)
    tuples.foreach(writer.putOne)

    // Nothing is committed until the buffer fills or the writer closes
    assert(readTuples(tableName).isEmpty)

    writer.close()
    assert(readTuples(tableName).sortBy(_.getField[Int]("id")) == tuples.toList)
  }

  it should "auto-flush when the buffer reaches the configured batch size" in {
    val tableName = freshTableName()
    val writer = createWriter(tableName)
    val batchSize = writer.bufferSize
    writer.open()
    (0 until batchSize).foreach(i => writer.putOne(tuple(i)))

    // The full batch is committed without an explicit close
    assert(readTuples(tableName).size == batchSize)

    writer.putOne(tuple(batchSize))
    writer.close()
    val tuples = readTuples(tableName)
    assert(tuples.size == batchSize + 1)
    // Each flush creates its own data file
    assert(loadTable(tableName).snapshots().asScala.size == 2)
  }

  it should "not write tuples removed from the buffer before a flush" in {
    val tableName = freshTableName()
    val writer = createWriter(tableName)
    writer.open()
    val kept = tuple(1)
    val removed = tuple(2)
    writer.putOne(kept)
    writer.putOne(removed)
    writer.removeOne(removed)
    writer.close()

    assert(readTuples(tableName) == List(kept))
  }

  it should "prefix created data files with the writer identifier" in {
    val tableName = freshTableName()
    val writer = createWriter(tableName, writerIdentifier = "worker_42")
    writer.open()
    writer.putOne(tuple(1))
    writer.close()

    val table = loadTable(tableName)
    val dataFiles = table
      .currentSnapshot()
      .addedDataFiles(table.io())
      .asScala
      .map(_.location())
      .toList
    assert(dataFiles.nonEmpty)
    assert(dataFiles.forall(_.contains("worker_42_")))
  }
  it should "resolve its table from the catalog installed at flush time, not at construction" in {
    // #7290: an eagerly-held Table would keep the writer bound to the catalog that was
    // cached when it was constructed -- a reference the bounded cache may evict and
    // close. Swapping the warehouse entry after construction must redirect the flush.
    val swapWarehouse = "iceberg-table-writer-spec-swap"
    val tableName = s"tbl_${UUID.randomUUID().toString.replace("-", "")}"
    IcebergUtil.createTable(
      catalog,
      tableNamespace,
      tableName,
      icebergSchema,
      overrideIfExists = true
    )

    val constructionCatalog = new CountingCatalog(catalog)
    IcebergCatalogInstance.replaceInstance(constructionCatalog, Some(swapWarehouse))
    val writer = new IcebergTableWriter[Tuple](
      "writer_swap",
      Some(swapWarehouse),
      tableNamespace,
      tableName,
      icebergSchema,
      IcebergUtil.toGenericRecord
    )
    writer.open()
    writer.putOne(tuple(1))

    // Install a different catalog before the flush; the writer must go through it.
    val flushCatalog = new CountingCatalog(catalog)
    IcebergCatalogInstance.replaceInstance(flushCatalog, Some(swapWarehouse))
    val loadsBeforeFlush = flushCatalog.loadTableCalls.get()

    writer.close() // flushes the buffer

    assert(
      flushCatalog.loadTableCalls.get() > loadsBeforeFlush,
      "the flush must load the table through the catalog installed at flush time"
    )
    assert(readTuples(tableName) == List(tuple(1)), "the tuple must reach the table")
  }

  /** Delegates to `delegate` while counting `loadTable` calls. */
  private class CountingCatalog(delegate: Catalog) extends Catalog {
    val loadTableCalls = new AtomicInteger()
    override def name(): String = "counting"
    override def loadTable(identifier: TableIdentifier): Table = {
      loadTableCalls.incrementAndGet()
      delegate.loadTable(identifier)
    }
    override def tableExists(identifier: TableIdentifier): Boolean =
      delegate.tableExists(identifier)
    override def listTables(namespace: Namespace): java.util.List[TableIdentifier] =
      delegate.listTables(namespace)
    override def dropTable(identifier: TableIdentifier, purge: Boolean): Boolean =
      delegate.dropTable(identifier, purge)
    override def renameTable(from: TableIdentifier, to: TableIdentifier): Unit =
      delegate.renameTable(from, to)
  }
}
