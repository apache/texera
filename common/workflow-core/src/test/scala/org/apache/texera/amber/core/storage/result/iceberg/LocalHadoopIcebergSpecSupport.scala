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

import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.{Schema => IcebergSchema, Table}
import org.apache.texera.amber.core.storage.IcebergCatalogInstance
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.IcebergUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters._

/** A spec's own Hadoop catalog in a temporary `file:/` warehouse, with the table, writer and
  * tuple fixtures its specs share. The writer resolves its catalog from the shared cache per use
  * (#7290), so each spec registers its catalog under a spec-unique warehouse name.
  */
private[iceberg] trait LocalHadoopIcebergSpecSupport extends AnyFlatSpec with BeforeAndAfterAll {

  protected def tableNamespace: String
  protected def specWarehouse: String
  protected def catalogName: String = specWarehouse

  protected var catalog: Catalog = _

  protected val amberSchema: Schema = Schema()
    .add("id", AttributeType.INTEGER)
    .add("name", AttributeType.STRING)

  protected val icebergSchema: IcebergSchema = IcebergUtil.toIcebergSchema(amberSchema)

  override def beforeAll(): Unit = {
    super.beforeAll()
    catalog = IcebergUtil.createHadoopCatalog(catalogName, Files.createTempDirectory(specWarehouse))
    IcebergCatalogInstance.replaceInstance(catalog, Some(specWarehouse))
  }

  override def afterAll(): Unit = {
    catalog match {
      case closeable: AutoCloseable => closeable.close()
      case _                        =>
    }
    super.afterAll()
  }

  /** A name no other table or writer in the run uses, starting with `prefix`. */
  protected def freshTableName(prefix: String = "table"): String =
    s"${prefix}_${UUID.randomUUID().toString.replace("-", "")}"

  protected def createTable(tableName: String): Unit =
    IcebergUtil.createTable(
      catalog,
      tableNamespace,
      tableName,
      icebergSchema,
      overrideIfExists = true
    )

  protected def newWriter(
      tableName: String,
      writerIdentifier: String,
      warehouse: String = specWarehouse
  ): IcebergTableWriter[Tuple] =
    new IcebergTableWriter[Tuple](
      writerIdentifier,
      Some(warehouse),
      tableNamespace,
      tableName,
      icebergSchema,
      IcebergUtil.toGenericRecord
    )

  protected def loadTable(tableName: String): Table =
    IcebergUtil.loadTableMetadata(catalog, tableNamespace, tableName).get

  protected def tuple(id: Int): Tuple =
    Tuple
      .builder(amberSchema)
      .addSequentially(Array(Int.box(id), s"name-$id"))
      .build()

  protected def readTuples(tableName: String): List[Tuple] = {
    val records = IcebergGenerics.read(loadTable(tableName)).build()
    try {
      records.iterator().asScala.map(IcebergUtil.fromRecord(_, amberSchema)).toList
    } finally {
      records.close()
    }
  }
}
