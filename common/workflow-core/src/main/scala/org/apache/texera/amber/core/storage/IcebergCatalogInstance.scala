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

import com.google.common.base.Ticker
import com.google.common.cache.{
  Cache,
  CacheBuilder,
  RemovalCause,
  RemovalListener,
  RemovalNotification
}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog

import java.time.Duration
import java.util.concurrent.{Callable, ExecutionException}
import scala.util.Try

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
  * The cache is bounded (#7290): per-user warehouses (#6870) make the set of catalogs a
  * long-lived JVM touches unbounded, and each REST catalog holds an HTTP client. An entry
  * idle for the expiry window is closed -- nothing can be using it, and idle entries are
  * exactly what a long-lived JVM accumulates. An entry evicted by *size* is only dropped,
  * never closed: size pressure means more simultaneously hot warehouses than the bound,
  * and closing a hot catalog would fail the operations still using it. Load degrades into
  * rebuild churn (the dropped catalog decays once its in-flight users finish), not errors.
  *
  * Callers must therefore resolve their catalog per logical operation instead of holding
  * one across an execution -- that is also what keeps a dropped catalog's lifetime bounded
  * by the operation using it (see IcebergDocument / IcebergTableWriter).
  *
  * Only *evicted* entries are closed. A catalog displaced by [[replaceInstance]] is the
  * caller's to manage: whoever replaces an entry may still hold (and restore) the old
  * reference -- tests wrap-and-restore the shared catalog, and endpoint reconfiguration
  * (#7358) will swap catalogs the same way.
  */
object IcebergCatalogInstance extends LazyLogging {

  // Sizing mirrors HuggingFaceModelResource's bounded-cache precedent: generous enough
  // that eviction never hits a warehouse in active use, small enough to bound the JVM.
  private val CatalogCacheMaxSize = 64L
  private val CatalogCacheExpireAfterAccess = Duration.ofMinutes(60)

  /**
    * Builds a catalog cache with the eviction wiring `getInstance` relies on.
    * Package-private so the spec can exercise size and idle eviction on isolated
    * instances with a manual ticker, instead of flooding the JVM-wide cache below.
    */
  private[storage] def buildCatalogCache(
      maximumSize: Long,
      expireAfterAccess: Duration,
      ticker: Ticker
  ): Cache[String, Catalog] =
    CacheBuilder
      .newBuilder()
      .maximumSize(maximumSize)
      .expireAfterAccess(expireAfterAccess)
      .ticker(ticker)
      .removalListener(new RemovalListener[String, Catalog] {
        override def onRemoval(notification: RemovalNotification[String, Catalog]): Unit =
          // Close ONLY idle-expired entries. A size-evicted catalog may be mid-operation
          // (overload = more hot warehouses than the bound) and a replaced one is still
          // the replacing caller's (wrap-and-restore in tests, reconfiguration later);
          // both are dropped un-closed and decay once their last user finishes.
          if (notification.getCause == RemovalCause.EXPIRED) {
            notification.getValue match {
              case closeable: AutoCloseable =>
                Try(closeable.close()).failed.foreach(error =>
                  logger.warn(s"failed to close expired catalog '${notification.getKey}'", error)
                )
              case _ =>
            }
          }
      })
      .build[String, Catalog]()

  private val catalogs: Cache[String, Catalog] =
    buildCatalogCache(CatalogCacheMaxSize, CatalogCacheExpireAfterAccess, Ticker.systemTicker())

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
    getOrLoad(catalogs, cacheKey(name), () => createCatalog(name))
  }

  /**
    * `Cache.get` wraps loader failures (`UncheckedExecutionException`, `ExecutionException`,
    * `ExecutionError`); unwrap them so `createCatalog` failures keep the types they had
    * before the cache existed. Package-private so the spec can pin the unwrapping against
    * an isolated cache with a throwing loader.
    */
  private[storage] def getOrLoad(
      cache: Cache[String, Catalog],
      key: String,
      loader: () => Catalog
  ): Catalog =
    try {
      // get(key, loader) locks per key, not globally: a cache miss's REST config
      // round trip no longer blocks lookups of other warehouses.
      cache.get(
        key,
        new Callable[Catalog] {
          override def call(): Catalog = loader()
        }
      )
    } catch {
      case e @ (_: UncheckedExecutionException | _: ExecutionException | _: ExecutionError) =>
        throw e.getCause
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
    catalogs.put(cacheKey(warehouse.getOrElse(defaultWarehouse)), catalog)
}
