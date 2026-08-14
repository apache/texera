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
import org.apache.texera.amber.core.storage.result.iceberg.IcebergDocument
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.IcebergUtil
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration

/**
  * Spec for the bounded catalog cache (#7290): only *idle-expired* entries are closed.
  * A size-evicted catalog is dropped un-closed (it may be mid-operation under load),
  * a catalog displaced by replaceInstance stays open (the replacing caller may still
  * hold and later restore it -- wrap-and-restore, as amber's integration
  * IcebergDocumentSpec does), and holders resolve their catalog per operation so a
  * replacement is visible immediately.
  *
  * Size and idle eviction are exercised on isolated caches built through the
  * package-private factory (with a manual ticker), never on the JVM-wide cache
  * that parallel suites share. Tests that do touch the shared cache use their
  * own spec-unique warehouse keys.
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

  /** A ticker the tests advance by hand, making idle expiry deterministic. */
  private class ManualTicker extends Ticker {
    @volatile private var nanos = 0L
    def advance(duration: Duration): Unit = nanos += duration.toNanos
    override def read(): Long = nanos
  }

  "the catalog cache" should "drop entries beyond the size bound without closing them" in {
    // Size pressure means more simultaneously hot warehouses than the bound; the
    // evicted catalog may be mid-operation, so it must decay via GC, never be closed.
    val cache =
      IcebergCatalogInstance.buildCatalogCache(2, Duration.ofMinutes(60), new ManualTicker)
    val fakes = (1 to 3).map(i => new FakeCatalog(s"size-$i"))

    fakes.zipWithIndex.foreach { case (fake, i) => cache.put(s"warehouse-$i", fake) }

    cache.size() should be <= 2L
    fakes.count(_.closed) shouldBe 0
  }

  it should "close an entry left idle beyond the expiry window" in {
    val ticker = new ManualTicker
    val cache = IcebergCatalogInstance.buildCatalogCache(64, Duration.ofMinutes(60), ticker)
    val idle = new FakeCatalog("idle")
    cache.put("idle", idle)

    ticker.advance(Duration.ofMinutes(61))
    // Reads alone may defer removal processing; cleanUp() drains it deterministically.
    cache.cleanUp()

    cache.getIfPresent("idle") shouldBe null
    idle.closed shouldBe true
  }

  it should "tolerate a catalog whose close fails, and still drop the entry" in {
    val ticker = new ManualTicker
    val cache = IcebergCatalogInstance.buildCatalogCache(64, Duration.ofMinutes(60), ticker)
    val faulty = new FakeCatalog("faulty") {
      override def close(): Unit = throw new IllegalStateException("close failed")
    }
    cache.put("faulty", faulty)

    ticker.advance(Duration.ofMinutes(61))
    noException should be thrownBy cache.cleanUp()

    cache.getIfPresent("faulty") shouldBe null
  }

  it should "leave a catalog that is not closable alone when it expires" in {
    // Hadoop/postgres catalogs need not implement AutoCloseable; expiry must not fail.
    val ticker = new ManualTicker
    val cache = IcebergCatalogInstance.buildCatalogCache(64, Duration.ofMinutes(60), ticker)
    val notClosable = new Catalog {
      override def name(): String = "not-closable"
      override def listTables(namespace: Namespace): java.util.List[TableIdentifier] =
        throw new UnsupportedOperationException
      override def dropTable(identifier: TableIdentifier, purge: Boolean): Boolean =
        throw new UnsupportedOperationException
      override def renameTable(from: TableIdentifier, to: TableIdentifier): Unit =
        throw new UnsupportedOperationException
      override def loadTable(identifier: TableIdentifier): Table =
        throw new UnsupportedOperationException
    }
    cache.put("not-closable", notClosable)

    ticker.advance(Duration.ofMinutes(61))
    noException should be thrownBy cache.cleanUp()

    cache.getIfPresent("not-closable") shouldBe null
  }

  it should "surface loader failures with their original exception type" in {
    val cache =
      IcebergCatalogInstance.buildCatalogCache(64, Duration.ofMinutes(60), new ManualTicker)

    // Guava wraps a runtime failure in UncheckedExecutionException and a checked one
    // in ExecutionException; getOrLoad must rethrow the original in both cases.
    val runtimeFailure = intercept[IllegalArgumentException] {
      IcebergCatalogInstance.getOrLoad(
        cache,
        "unsupported",
        () => throw new IllegalArgumentException("Unsupported catalog type")
      )
    }
    runtimeFailure.getMessage should include("Unsupported catalog type")

    an[java.io.IOException] should be thrownBy
      IcebergCatalogInstance.getOrLoad(
        cache,
        "unreachable",
        () => throw new java.io.IOException("connection refused")
      )

    // Errors ride the third wrapper, ExecutionError.
    an[StackOverflowError] should be thrownBy
      IcebergCatalogInstance.getOrLoad(cache, "fatal", () => throw new StackOverflowError("boom"))
  }

  "getInstance" should "return the catalog installed for its warehouse" in {
    val installed = new FakeCatalog("installed")
    IcebergCatalogInstance.replaceInstance(installed, Some("catalog-cache-spec-get"))

    IcebergCatalogInstance.getInstance(Some("catalog-cache-spec-get")) should be theSameInstanceAs
      installed
  }

  "replaceInstance" should "leave the displaced catalog open for its owner (wrap-and-restore)" in {
    // Integration tests wrap the shared catalog in a spy and restore it afterwards;
    // closing the displaced instance would hand back a dead catalog (#7290 review).
    val original = new FakeCatalog("original")
    val wrapper = new FakeCatalog("wrapper")
    IcebergCatalogInstance.replaceInstance(original, Some("catalog-cache-spec-replace"))

    IcebergCatalogInstance.replaceInstance(wrapper, Some("catalog-cache-spec-replace"))
    original.closed shouldBe false

    IcebergCatalogInstance.replaceInstance(original, Some("catalog-cache-spec-replace"))
    wrapper.closed shouldBe false
    IcebergCatalogInstance.getInstance(
      Some("catalog-cache-spec-replace")
    ) should be theSameInstanceAs
      original
  }

  it should "keep a re-registered shared instance open" in {
    // LocalHadoopIcebergCatalog.ensure re-puts one shared instance from every suite
    // (and under several warehouse names); none of that may close it.
    val shared = new FakeCatalog("shared")
    IcebergCatalogInstance.replaceInstance(shared, Some("catalog-cache-spec-idempotent"))

    IcebergCatalogInstance.replaceInstance(shared, Some("catalog-cache-spec-idempotent"))

    shared.closed shouldBe false
    IcebergCatalogInstance.getInstance(Some("catalog-cache-spec-idempotent")) should
      be theSameInstanceAs shared
  }

  "IcebergDocument.clear" should "address one catalog for the whole check-then-drop" in {
    // Per-use resolution means per logical operation, not per call: the fake below
    // swaps the cache entry from INSIDE the existence check, and the drop must still
    // land on the catalog the operation started with (#7290 review, round 2).
    class ImpostorCatalog extends FakeCatalog("impostor") {
      @volatile var dropCalls = 0
      override def tableExists(identifier: TableIdentifier): Boolean = true
      override def dropTable(identifier: TableIdentifier, purge: Boolean): Boolean = {
        dropCalls += 1; true
      }
    }
    class SwappingCatalog(impostor: ImpostorCatalog) extends FakeCatalog("swapping") {
      @volatile var dropCalls = 0
      override def tableExists(identifier: TableIdentifier): Boolean = {
        IcebergCatalogInstance.replaceInstance(impostor, Some("catalog-cache-spec-clear"))
        true
      }
      override def dropTable(identifier: TableIdentifier, purge: Boolean): Boolean = {
        dropCalls += 1; true
      }
    }
    val impostor = new ImpostorCatalog
    val swapping = new SwappingCatalog(impostor)
    IcebergCatalogInstance.replaceInstance(swapping, Some("catalog-cache-spec-clear"))
    val amberSchema = Schema().add("id", AttributeType.INTEGER)
    val document = new IcebergDocument[Tuple](
      "catalog_cache_spec",
      "clear_probe",
      IcebergUtil.toIcebergSchema(amberSchema),
      IcebergUtil.toGenericRecord,
      (schema, record) => IcebergUtil.fromRecord(record, IcebergUtil.fromIcebergSchema(schema)),
      Some("catalog-cache-spec-clear")
    )

    document.clear()

    swapping.dropCalls shouldBe 1
    impostor.dropCalls shouldBe 0
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
    def catalogSeenAfterInstalling(catalog: Catalog): Catalog = {
      IcebergCatalogInstance.replaceInstance(catalog, Some("catalog-cache-spec-swap"))
      document.catalog
    }
    val before = new FakeCatalog("before")
    val after = new FakeCatalog("after")

    catalogSeenAfterInstalling(before) should be theSameInstanceAs before
    catalogSeenAfterInstalling(after) should be theSameInstanceAs after
  }
}
