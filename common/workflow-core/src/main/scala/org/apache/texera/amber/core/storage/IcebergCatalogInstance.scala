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

package org.apache.texera.amber.core.storage

import org.apache.texera.common.config.StorageConfig
import org.apache.texera.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog

import scala.collection.mutable

/**
  * IcebergCatalogInstance manages the Iceberg catalog clients used across the Texera application.
  *
  * Catalogs are cached per warehouse: each distinct warehouse name gets its own lazily-created
  * catalog client, so a single JVM may hold several catalogs, one per warehouse it touches. Callers
  * that do not specify a warehouse use the configured default, preserving single-warehouse (non-BYO)
  * behavior.
  *
  * Only the REST catalog varies by warehouse; the hadoop and postgres catalogs are warehouse-agnostic
  * and ignore the warehouse argument.
  *
  * Access is synchronized because the same JVM serves multiple warehouses concurrently.
  */
object IcebergCatalogInstance {

  private val catalogs = mutable.Map.empty[String, Catalog]

  // Cache key for the warehouse-agnostic catalog types. Not a legal warehouse name,
  // so it cannot collide with a REST warehouse.
  private val SharedCatalogKey = "<shared>"

  private def defaultWarehouse: String = StorageConfig.icebergRESTCatalogWarehouseName

  /**
    * The cache key for a warehouse. Only the REST catalog is scoped to a warehouse;
    * hadoop and postgres ignore it, so they must share one entry. Keying them by
    * warehouse name would build a second, fully equivalent catalog per distinct name
    * -- for postgres a second JdbcCatalog with its own connection pool, all pointing
    * at the same database. Mirrors the Python side, which keys those under a constant.
    */
  private def cacheKey(warehouse: String): String =
    StorageConfig.icebergCatalogType match {
      case "rest" => warehouse
      case _      => SharedCatalogKey
    }

  /**
    * Retrieves the catalog for the given warehouse, creating and caching it on first access.
    *
    * @param warehouse the warehouse to obtain a catalog for; `None` uses the configured
    *                  default, mirroring the Python side's `Optional[str]`.
    * @return the Iceberg catalog for that warehouse.
    */
  def getInstance(warehouse: Option[String] = None): Catalog = {
    val name = warehouse.getOrElse(defaultWarehouse)
    synchronized {
      catalogs.getOrElseUpdate(cacheKey(name), createCatalog(name))
    }
  }

  private def createCatalog(warehouse: String): Catalog =
    StorageConfig.icebergCatalogType match {
      case "hadoop" =>
        IcebergUtil.createHadoopCatalog(
          "texera_iceberg",
          StorageConfig.fileStorageDirectoryPath
        )
      case "rest" =>
        IcebergUtil.createRestCatalog(
          "texera_iceberg",
          warehouse
        )
      case "postgres" =>
        IcebergUtil.createPostgresCatalog(
          "texera_iceberg",
          StorageConfig.fileStorageDirectoryPath
        )
      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported catalog type: $unsupported")
    }

  /**
    * Replaces the cached catalog for a warehouse, primarily for testing or reconfiguration.
    *
    * @param catalog   the catalog to cache.
    * @param warehouse the warehouse to cache it under; `None` uses the configured default.
    */
  def replaceInstance(catalog: Catalog, warehouse: Option[String] = None): Unit =
    synchronized {
      catalogs(cacheKey(warehouse.getOrElse(defaultWarehouse))) = catalog
    }
}
