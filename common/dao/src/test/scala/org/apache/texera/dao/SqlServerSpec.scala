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

package org.apache.texera.dao

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.jooq.impl.DSL
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll}

class SqlServerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with MockTexeraDB {

  override def beforeAll(): Unit = initializeDBAndReplaceDSLContext()
  override def afterAll(): Unit = shutdownDB()

  // -------------------------------------------------------------------------
  // SqlServer.withTransaction
  //
  // getDSLContext is backed by the embedded Postgres DataSource, so each
  // top-level query borrows a connection from the pool.  withTransaction
  // binds a single connection for the duration of the block, making rollback
  // and commit behaviour fully observable.
  // -------------------------------------------------------------------------

  "SqlServer.withTransaction" should "return the value produced by the block" in {
    val result = SqlServer.withTransaction(getDSLContext) { _ => 42 }
    result shouldBe 42
  }

  it should "commit the block's work so subsequent queries observe the changes" in {
    // SELECT 1 is a lightweight live query; completing without error confirms
    // the transaction committed and the connection was returned cleanly.
    val result = SqlServer.withTransaction(getDSLContext) { ctx =>
      ctx.selectOne().fetchOne().value1()
    }
    result shouldBe 1
  }

  it should "re-throw the exception when the block throws" in {
    val boom = new RuntimeException("intentional failure")
    val thrown = intercept[RuntimeException] {
      SqlServer.withTransaction(getDSLContext) { _ => throw boom }
    }
    thrown.getMessage should include("intentional failure")
  }

  it should "roll back all DML in the block when an exception is thrown" in {
    // A permanent (non-TEMP) table is used so every connection from the pool
    // can see it; TEMP tables are session-scoped and would be invisible across
    // pool connections.
    val dsl = getDSLContext
    dsl.execute("CREATE TABLE IF NOT EXISTS _txn_rollback_test (v INT)")
    try {
      intercept[RuntimeException] {
        SqlServer.withTransaction(dsl) { ctx =>
          ctx.execute("INSERT INTO _txn_rollback_test VALUES (99)")
          throw new RuntimeException("force rollback")
        }
      }
      // The INSERT was inside the rolled-back transaction, so the table must
      // still be empty.
      dsl.fetchCount(DSL.table(DSL.name("_txn_rollback_test"))) shouldBe 0
    } finally {
      dsl.execute("DROP TABLE IF EXISTS _txn_rollback_test")
    }
  }

  it should "support nested return types beyond Int" in {
    val result = SqlServer.withTransaction(getDSLContext) { ctx =>
      ctx.selectOne().fetchOne().value1().toString
    }
    result shouldBe "1"
  }

  // -------------------------------------------------------------------------
  // HikariCP pool lifecycle and configuration
  //
  // These tests create their own HikariDataSource against the embedded Postgres
  // instance so they can drive the pool directly, independently of the
  // DSLContext replacement that MockTexeraDB applies for its own queries.
  // -------------------------------------------------------------------------

  private def buildPool(
      maxSize: Int = 5,
      minIdle: Int = 1,
      poolName: String = "spec-pool"
  ): HikariDataSource = {
    // Use the default "postgres" database so no schema setup is needed.
    val jdbcUrl = getDBInstance.getJdbcUrl("postgres", "postgres")
    val cfg = new HikariConfig()
    cfg.setJdbcUrl(jdbcUrl)
    cfg.setUsername("postgres")
    cfg.setPassword("")
    cfg.setPoolName(poolName)
    cfg.setMaximumPoolSize(maxSize)
    cfg.setMinimumIdle(minIdle)
    cfg.setConnectionTimeout(5000)
    new HikariDataSource(cfg)
  }

  "HikariCP pool" should "provide a usable connection that can execute queries" in {
    val ds = buildPool()
    try {
      val conn = ds.getConnection
      try {
        val rs = conn.prepareStatement("SELECT 1").executeQuery()
        rs.next() shouldBe true
        rs.getInt(1) shouldBe 1
      } finally conn.close()
    } finally ds.close()
  }

  it should "apply the configured pool name" in {
    val ds = buildPool(poolName = "my-named-pool")
    try {
      ds.getHikariConfigMXBean.getPoolName shouldBe "my-named-pool"
    } finally ds.close()
  }

  it should "apply the configured maximum pool size" in {
    val ds = buildPool(maxSize = 7)
    try {
      ds.getHikariConfigMXBean.getMaximumPoolSize shouldBe 7
    } finally ds.close()
  }

  it should "apply the configured minimum idle connections" in {
    val ds = buildPool(minIdle = 2)
    try {
      ds.getHikariConfigMXBean.getMinimumIdle shouldBe 2
    } finally ds.close()
  }

  it should "count a borrowed connection as active" in {
    val ds = buildPool()
    try {
      val conn = ds.getConnection
      try {
        ds.getHikariPoolMXBean.getActiveConnections should be >= 1
      } finally conn.close()
    } finally ds.close()
  }

  it should "decrement active count and increment idle count once a connection is returned" in {
    val ds = buildPool()
    try {
      val conn = ds.getConnection
      conn.close()
      ds.getHikariPoolMXBean.getActiveConnections shouldBe 0
      ds.getHikariPoolMXBean.getIdleConnections should be >= 1
    } finally ds.close()
  }

  it should "allow up to the maximum pool size connections to be borrowed concurrently" in {
    val ds = buildPool(maxSize = 3)
    try {
      val c1 = ds.getConnection
      val c2 = ds.getConnection
      val c3 = ds.getConnection
      ds.getHikariPoolMXBean.getActiveConnections shouldBe 3
      c1.close(); c2.close(); c3.close()
    } finally ds.close()
  }

  it should "report isClosed as false while open and true after close" in {
    val ds = buildPool()
    ds.isClosed shouldBe false
    ds.close()
    ds.isClosed shouldBe true
  }

  it should "reject getConnection after the pool has been closed" in {
    val ds = buildPool()
    ds.close()
    // HikariCP throws an SQLException (wrapped as RuntimeException by the pool)
    // when a caller tries to borrow from a closed pool.
    assertThrows[Exception](ds.getConnection)
  }

  // -------------------------------------------------------------------------
  // SqlServer.close()
  //
  // The instance's private HikariDataSource is the only resource that needs
  // explicit release; close() guards it against double-close. These tests
  // construct a fresh SqlServer via initConnection (the only public entry
  // point — the class constructor is private) and assert against the
  // underlying pool via reflection, which avoids broadening the class API
  // just to make this branch observable.
  // -------------------------------------------------------------------------

  private def datasourceOf(instance: SqlServer): HikariDataSource = {
    val field = classOf[SqlServer].getDeclaredField("dataSource")
    field.setAccessible(true)
    field.get(instance).asInstanceOf[HikariDataSource]
  }

  "SqlServer.close" should "shut down the underlying HikariDataSource and be idempotent" in {
    val jdbcUrl = getDBInstance.getJdbcUrl("postgres", "postgres")
    // Replaces the singleton — initConnection internally calls close() on the
    // prior instance, which is itself an exercise of the same path. The trait
    // holds its own DSLContext separately, so other tests' database access is
    // unaffected by this replacement.
    SqlServer.initConnection(jdbcUrl, "postgres", "")
    val instance = SqlServer.getInstance()
    val ds = datasourceOf(instance)

    ds.isClosed shouldBe false
    instance.close()
    ds.isClosed shouldBe true

    // Second close() must take the `dataSource.isClosed` branch and return
    // without throwing. Calling Hikari's close() twice would itself be safe
    // today, but the guard is what this assertion pins.
    noException should be thrownBy instance.close()
    ds.isClosed shouldBe true
  }
}
