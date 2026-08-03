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
import org.apache.texera.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.{Schema => IcebergSchema, Table}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.jdk.CollectionConverters._

/**
  * Unit tests for the [[OnIceberg]] trait's snapshot-expiration behavior, exercised
  * through a minimal concrete implementation and a local Hadoop-backed catalog
  * (temp `file:/` warehouse), so no REST catalog or S3 endpoint is needed.
  */
class OnIcebergSpec extends AnyFlatSpec with BeforeAndAfterAll {

  /** The smallest possible OnIceberg implementation: it only supplies the three abstract members. */
  private case class TestOnIceberg(
      catalog: Catalog,
      tableNamespace: String,
      tableName: String
  ) extends OnIceberg

  private val tableNamespace = "on_iceberg_spec"
  private var warehouseDir: Path = _
  private var catalog: Catalog = _

  private val amberSchema: Schema = Schema()
    .add("id", AttributeType.INTEGER)
    .add("name", AttributeType.STRING)

  private val icebergSchema: IcebergSchema = IcebergUtil.toIcebergSchema(amberSchema)

  override def beforeAll(): Unit = {
    warehouseDir = Files.createTempDirectory("on-iceberg-spec")
    catalog = IcebergUtil.createHadoopCatalog("on-iceberg-spec", warehouseDir)
  }

  override def afterAll(): Unit = {
    catalog match {
      case closeable: AutoCloseable => closeable.close()
      case _                        =>
    }
  }

  private def freshTableName(): String =
    s"table_${UUID.randomUUID().toString.replace("-", "")}"

  private def createTable(tableName: String): Unit = {
    IcebergUtil.createTable(
      catalog,
      tableNamespace,
      tableName,
      icebergSchema,
      overrideIfExists = true
    )
  }

  private def loadTable(tableName: String): Table =
    IcebergUtil.loadTableMetadata(catalog, tableNamespace, tableName).get

  /** Commits one snapshot holding the given ids. */
  private def appendSnapshot(tableName: String, ids: Seq[Int]): Unit = {
    val writer = new IcebergTableWriter[Tuple](
      s"writer_${UUID.randomUUID().toString.replace("-", "")}",
      catalog,
      tableNamespace,
      tableName,
      icebergSchema,
      IcebergUtil.toGenericRecord
    )
    writer.open()
    ids.foreach(id =>
      writer.putOne(
        Tuple.builder(amberSchema).addSequentially(Array(Int.box(id), s"name-$id")).build()
      )
    )
    writer.close()
  }

  private def snapshotCount(tableName: String): Int =
    loadTable(tableName).snapshots().asScala.size

  private def readIds(tableName: String): List[Int] = {
    val records = IcebergGenerics.read(loadTable(tableName)).build()
    try {
      records
        .iterator()
        .asScala
        .map(IcebergUtil.fromRecord(_, amberSchema).getField[Int]("id"))
        .toList
    } finally {
      records.close()
    }
  }

  "OnIceberg.expireSnapshots" should "retain only the most recent snapshot" in {
    val tableName = freshTableName()
    createTable(tableName)
    appendSnapshot(tableName, Seq(1, 2))
    appendSnapshot(tableName, Seq(3))
    appendSnapshot(tableName, Seq(4))
    assert(snapshotCount(tableName) == 3)

    TestOnIceberg(catalog, tableNamespace, tableName).expireSnapshots()

    assert(snapshotCount(tableName) == 1)
  }

  it should "keep every live row of the surviving snapshot" in {
    val tableName = freshTableName()
    createTable(tableName)
    appendSnapshot(tableName, Seq(1, 2))
    appendSnapshot(tableName, Seq(3))

    TestOnIceberg(catalog, tableNamespace, tableName).expireSnapshots()

    // expiring snapshots drops history, not data: rows appended by the expired
    // snapshots are still referenced by the retained one
    assert(readIds(tableName).sorted == List(1, 2, 3))
    assert(snapshotCount(tableName) == 1)
  }

  it should "be idempotent when run twice" in {
    val tableName = freshTableName()
    createTable(tableName)
    appendSnapshot(tableName, Seq(1))
    appendSnapshot(tableName, Seq(2))

    val onIceberg = TestOnIceberg(catalog, tableNamespace, tableName)
    onIceberg.expireSnapshots()
    onIceberg.expireSnapshots()

    assert(snapshotCount(tableName) == 1)
    assert(readIds(tableName).sorted == List(1, 2))
  }

  it should "be a no-op for a table that has never been written to" in {
    val tableName = freshTableName()
    createTable(tableName)
    assert(snapshotCount(tableName) == 0)

    TestOnIceberg(catalog, tableNamespace, tableName).expireSnapshots()

    assert(snapshotCount(tableName) == 0)
    assert(readIds(tableName).isEmpty)
  }

  it should "throw NoSuchTableException naming the missing table" in {
    val missing = freshTableName()
    val ex = intercept[NoSuchTableException] {
      TestOnIceberg(catalog, tableNamespace, missing).expireSnapshots()
    }
    assert(ex.getMessage == s"table $tableNamespace.$missing doesn't exist")
  }
}
