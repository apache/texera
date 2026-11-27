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
import org.apache.texera.amber.core.tuple.Tuple

import scala.jdk.CollectionConverters._

class IcebergTableWriterSpec extends LocalHadoopIcebergSpecSupport {

  override protected val tableNamespace = "writer_spec"
  override protected val specWarehouse = "iceberg-table-writer-spec"
  override protected val catalogName = "writer-spec"

  private def createWriter(
      tableName: String,
      writerIdentifier: String = "writer_0"
  ): IcebergTableWriter[Tuple] = {
    createTable(tableName)
    newWriter(tableName, writerIdentifier)
  }

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
    val tableName = freshTableName("tbl")
    createTable(tableName)

    val constructionCatalog = new CountingCatalog(catalog)
    IcebergCatalogInstance.replaceInstance(constructionCatalog, Some(swapWarehouse))
    val writer = newWriter(tableName, writerIdentifier = "writer_swap", warehouse = swapWarehouse)
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

  it should "commit zero snapshots when opened and closed with an empty buffer" in {
    val tableName = freshTableName()
    val writer = createWriter(tableName)
    writer.open()
    writer.close()
    assert(loadTable(tableName).snapshots().asScala.isEmpty)
  }

}
