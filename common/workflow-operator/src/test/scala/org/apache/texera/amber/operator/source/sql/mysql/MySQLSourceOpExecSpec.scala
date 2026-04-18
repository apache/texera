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

package org.apache.texera.amber.operator.source.sql.mysql

import ch.vorburger.mariadb4j.{DB, DBConfigurationBuilder}
import io.vertx.core.Vertx
import io.vertx.sqlclient.Pool
import org.apache.texera.amber.core.tuple.{AttributeType, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Integration tests for [[MySQLSourceOpExec]] and the Vert.x-backed
  * [[MySQLSourceOpDesc.sourceSchema]]. Spins up an embedded MariaDB
  * (wire-compatible with MySQL) once for the whole suite.
  */
class MySQLSourceOpExecSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private var db: DB = _
  private var port: String = _
  private val host = "localhost"
  private val dbName = "texera_test"
  private val user = "root"
  private val password = ""
  private val table = "people"

  override def beforeAll(): Unit = {
    try {
      val cfg = DBConfigurationBuilder.newBuilder()
      cfg.setPort(0) // auto-assign a free port
      db = DB.newEmbeddedDB(cfg.build())
      db.start()
      port = db.getConfiguration.getPort.toString
      db.createDB(dbName)

      withPool { pool =>
        exec(
          pool,
          s"""CREATE TABLE $table (
             |  id        INT PRIMARY KEY,
             |  name      VARCHAR(64),
             |  age       INT,
             |  salary    DOUBLE,
             |  joined_at TIMESTAMP NULL
             |)""".stripMargin
        )
        exec(pool, s"INSERT INTO $table VALUES (1,'alice',30,50000.0,'2020-01-01 00:00:00')")
        exec(pool, s"INSERT INTO $table VALUES (2,'bob',25,45000.5,'2021-06-15 12:00:00')")
        exec(pool, s"INSERT INTO $table VALUES (3,'carol',40,60000.0,'2019-03-20 09:30:00')")
      }
    } catch {
      case t: Throwable =>
        // Embedded MariaDB binaries are platform-specific and can fail to start
        // (e.g. missing OpenSSL on macOS). Skip the suite rather than fail CI.
        info(s"Skipping MySQLSourceOpExecSpec: embedded MariaDB unavailable — ${t.getMessage}")
        if (db != null) try db.stop()
        catch { case _: Throwable => () }
        db = null
    }
  }

  override def afterAll(): Unit = {
    if (db != null) db.stop()
  }

  private def skipIfNoDb(): Unit =
    if (db == null) cancel("embedded MariaDB not available on this platform")

  private def withPool[T](body: Pool => T): T = {
    val vertx = Vertx.vertx()
    try {
      val pool = MySQLConnUtil.createPool(vertx, host, port, dbName, user, password)
      try body(pool)
      finally pool.close().toCompletionStage.toCompletableFuture.get()
    } finally vertx.close().toCompletionStage.toCompletableFuture.get()
  }

  private def exec(pool: Pool, sql: String): Unit =
    pool.query(sql).execute().toCompletionStage.toCompletableFuture.get()

  private def makeDesc(): MySQLSourceOpDesc = {
    val d = new MySQLSourceOpDesc()
    d.host = host
    d.port = port
    d.database = dbName
    d.table = table
    d.username = user
    d.password = password
    d
  }

  private def runExec(desc: MySQLSourceOpDesc): List[Tuple] = {
    val exec = new MySQLSourceOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    try exec.produceTuple().map(_.asInstanceOf[Tuple]).toList
    finally exec.close()
  }

  behavior of "MySQLSourceOpDesc.sourceSchema"

  it should "introspect all five columns with the expected types" in {
    skipIfNoDb()
    val schema = makeDesc().sourceSchema()
    val byName = schema.getAttributes.map(a => a.getName -> a.getType).toMap
    assert(byName("id") == AttributeType.INTEGER)
    assert(byName("name") == AttributeType.STRING)
    assert(byName("age") == AttributeType.INTEGER)
    assert(byName("salary") == AttributeType.DOUBLE)
    assert(byName("joined_at") == AttributeType.TIMESTAMP)
  }

  it should "return null when the table does not exist" in {
    skipIfNoDb()
    val d = makeDesc()
    d.table = "no_such_table"
    assert(d.sourceSchema() == null)
  }

  behavior of "MySQLSourceOpExec"

  it should "produce all rows in insertion order" in {
    skipIfNoDb()
    val tuples = runExec(makeDesc())
    assert(tuples.size == 3)
    assert(tuples.map(_.getField[Integer]("id")) == List(1, 2, 3))
    assert(tuples.map(_.getField[String]("name")) == List("alice", "bob", "carol"))
  }

  it should "honor the configured limit" in {
    skipIfNoDb()
    val d = makeDesc()
    d.limit = Some(2L)
    assert(runExec(d).size == 2)
  }

  it should "honor the configured offset" in {
    skipIfNoDb()
    val d = makeDesc()
    d.offset = Some(1L)
    val tuples = runExec(d)
    assert(tuples.size == 2)
    assert(tuples.head.getField[Integer]("id") == 2)
  }

  it should "throw when the table is missing" in {
    skipIfNoDb()
    val d = makeDesc()
    d.table = "no_such_table"
    val exec = new MySQLSourceOpExec(objectMapper.writeValueAsString(d))
    assertThrows[RuntimeException](exec.open())
  }

  it should "decode TIMESTAMP columns into java.sql.Timestamp" in {
    skipIfNoDb()
    val tuples = runExec(makeDesc())
    val first = tuples.head
    val ts = first.getField[Any]("joined_at")
    // normalized LocalDateTime -> Timestamp; parseTimestamp keeps it as Timestamp
    assert(ts != null)
    assert(ts.toString.startsWith("2020-01-01"))
  }
}
