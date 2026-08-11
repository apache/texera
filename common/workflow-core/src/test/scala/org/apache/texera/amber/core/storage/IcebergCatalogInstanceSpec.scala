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

import org.apache.texera.amber.core.storage.result.iceberg.IcebergDocument
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.IcebergUtil
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for the bounded catalog cache (#7290): a genuine replacement closes the
  * catalog it displaces, a same-instance re-registration does not (that is what
  * [[LocalHadoopIcebergCatalog.ensure]] relies on), and holders resolve their
  * catalog per use so a replacement is visible immediately.
  *
  * Size-based eviction is deliberately not exercised: forcing it would flood the
  * JVM-wide cache that parallel suites share and could evict their live catalog.
  * The close-on-removal wiring it would exercise is pinned by the replacement
  * cases below, which Guava routes through the same removal listener.
  *
  * Every test uses its own spec-unique warehouse key, so nothing here can touch
  * another suite's cache entries.
  */
class IcebergCatalogInstanceSpec extends AnyFlatSpec with Matchers {

  /** A closable catalog stub; the cache only ever needs `close()` on eviction. */
  private class FakeCatalog(catalogName: String) extends Catalog with AutoCloseable {
    @volatile var closed = false
    override def close(): Unit = closed = true
    override def name(): String = catalogName
    override def listTables(namespace: Namespace): java.util.List[TableIdentifier] =
      throw new UnsupportedOperationException
    override def dropTable(identifier: TableIdentifier, purge: Boolean): Boolean =
      throw new UnsupportedOperationException
    override def renameTable(from: TableIdentifier, to: TableIdentifier): Unit =
      throw new UnsupportedOperationException
    override def loadTable(identifier: TableIdentifier): Table =
      throw new UnsupportedOperationException
  }

  "getInstance" should "return the catalog installed for its warehouse" in {
    val installed = new FakeCatalog("installed")
    IcebergCatalogInstance.replaceInstance(installed, Some("catalog-cache-spec-get"))

    IcebergCatalogInstance.getInstance(Some("catalog-cache-spec-get")) should be theSameInstanceAs
      installed
  }

  "replaceInstance" should "close the catalog it displaces" in {
    val first = new FakeCatalog("first")
    val second = new FakeCatalog("second")
    IcebergCatalogInstance.replaceInstance(first, Some("catalog-cache-spec-replace"))

    IcebergCatalogInstance.replaceInstance(second, Some("catalog-cache-spec-replace"))

    first.closed shouldBe true
    second.closed shouldBe false
    IcebergCatalogInstance.getInstance(
      Some("catalog-cache-spec-replace")
    ) should be theSameInstanceAs
      second
  }

  it should "not close a catalog that is re-registered unchanged" in {
    // LocalHadoopIcebergCatalog.ensure re-puts one shared instance from every suite
    // (and under several warehouse names); a same-instance put must stay a no-op.
    val shared = new FakeCatalog("shared")
    IcebergCatalogInstance.replaceInstance(shared, Some("catalog-cache-spec-idempotent"))

    IcebergCatalogInstance.replaceInstance(shared, Some("catalog-cache-spec-idempotent"))

    shared.closed shouldBe false
    IcebergCatalogInstance.getInstance(Some("catalog-cache-spec-idempotent")) should
      be theSameInstanceAs shared
  }

  "IcebergDocument" should "resolve its catalog per use, seeing a replacement immediately" in {
    // Pins the per-use `def` (#7290): a `lazy val` would keep returning the catalog
    // that was current at first access, i.e. a reference the cache may have closed.
    val amberSchema = Schema().add("id", AttributeType.INTEGER)
    val document = new IcebergDocument[Tuple](
      "catalog_cache_spec",
      "swap_probe",
      IcebergUtil.toIcebergSchema(amberSchema),
      IcebergUtil.toGenericRecord,
      (schema, record) => IcebergUtil.fromRecord(record, IcebergUtil.fromIcebergSchema(schema)),
      Some("catalog-cache-spec-swap")
    )
    val before = new FakeCatalog("before")
    val after = new FakeCatalog("after")

    IcebergCatalogInstance.replaceInstance(before, Some("catalog-cache-spec-swap"))
    document.catalog should be theSameInstanceAs before

    IcebergCatalogInstance.replaceInstance(after, Some("catalog-cache-spec-swap"))
    document.catalog should be theSameInstanceAs after
  }
}
