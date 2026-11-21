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

package org.apache.texera.amber.operator.source.sql

import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{
  Connection,
  DatabaseMetaData,
  Driver,
  DriverManager,
  DriverPropertyInfo,
  PreparedStatement,
  ResultSet,
  SQLException,
  SQLFeatureNotSupportedException
}
import java.util.Properties
import java.util.logging.Logger
import scala.collection.mutable

private[sql] trait JdbcSourceOpExecTestHarness extends MockFactory with BeforeAndAfterAll {
  this: AnyFlatSpec with Matchers =>

  protected def jdbcUrlPrefix: String
  protected def tableListingSql: String
  protected def listingDatabase: Option[String] = None

  /** Returns pre-armed connections for the dialect’s test URL prefix. */
  protected final class StubDriver extends Driver {
    private val armed = mutable.Queue.empty[Connection]
    var lastUrl: String = _
    var lastProperties: Properties = _

    def arm(connection: Connection): Unit = armed.enqueue(connection)

    override def connect(url: String, info: Properties): Connection = {
      if (!acceptsURL(url)) return null
      lastUrl = url
      lastProperties = info
      if (armed.isEmpty) throw new SQLException("stub driver: no connection armed for " + url)
      armed.dequeue()
    }

    override def acceptsURL(url: String): Boolean = url != null && url.startsWith(jdbcUrlPrefix)
    override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
      Array.empty
    override def getMajorVersion: Int = 1
    override def getMinorVersion: Int = 0
    override def jdbcCompliant(): Boolean = false
    override def getParentLogger: Logger = throw new SQLFeatureNotSupportedException()
  }

  protected lazy val stubDriver = new StubDriver

  override def beforeAll(): Unit = {
    super.beforeAll()
    DriverManager.registerDriver(stubDriver)
  }

  override def afterAll(): Unit = {
    DriverManager.deregisterDriver(stubDriver)
    super.afterAll()
  }

  /** A connection answering the `sourceSchema()` JDBC-metadata probe run in the constructor. */
  protected final def schemaConn(columns: Seq[(String, Int)]): Connection = {
    val conn = mock[Connection]
    val metaData = mock[DatabaseMetaData]
    val columnsRs = mock[ResultSet]
    // once inside the dialect’s connect method, once inside SQLSourceOpDesc.querySchema
    (conn.setReadOnly _).expects(true).twice()
    (conn.getMetaData _).expects().returning(metaData)
    (metaData
      .getColumns(_: String, _: String, _: String, _: String))
      .expects(null, null, "tbl", null)
      .returning(columnsRs)
    inSequence {
      columns.foreach {
        case (name, jdbcType) =>
          (columnsRs.next _).expects().returning(true)
          (columnsRs.getString(_: String)).expects("COLUMN_NAME").returning(name)
          (columnsRs.getInt(_: String)).expects("DATA_TYPE").returning(jdbcType)
      }
      (columnsRs.next _).expects().returning(false)
    }
    (conn.close _).expects()
    conn
  }

  /** Stubs the `information_schema` probe that `loadTableNames` issues. */
  protected final def expectTableListing(conn: Connection, tables: Seq[String]): Unit = {
    val statement = mock[PreparedStatement]
    val resultSet = mock[ResultSet]
    (conn.prepareStatement(_: String)).expects(tableListingSql).returning(statement)
    listingDatabase.foreach(database => (statement.setString _).expects(1, database))
    (statement.executeQuery: () => ResultSet).expects().returning(resultSet)
    inSequence {
      tables.foreach(_ => (resultSet.next _).expects().returning(true))
      (resultSet.next _).expects().returning(false)
    }
    inSequence {
      tables.foreach(table => (resultSet.getString(_: Int)).expects(1).returning(table))
    }
    (resultSet.close _).expects()
    (statement.close _).expects()
  }

  protected final def configuredDescriptorJson[D <: SQLSourceOpDesc](
      desc: D,
      host: String,
      port: String
  )(configure: D => Unit): String = {
    desc.host = host
    desc.port = port
    desc.database = "db"
    desc.table = "tbl"
    desc.username = "u"
    desc.password = "p"
    configure(desc)
    objectMapper.writeValueAsString(desc)
  }

  protected final def connectionForOpen(tables: Seq[String]): Connection = {
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true)
    expectTableListing(conn, tables)
    conn
  }

  protected val baseSelect = "\nSELECT * FROM tbl where 1 = 1"

  protected trait QueryAccess {
    def loadTables(): Unit
    def filterClause: String
    def sqlQuery: Option[String]
  }

  protected final def verifyReconnect(expectedUrl: String)(reconnect: => Connection): Unit = {
    stubDriver.lastUrl shouldBe expectedUrl
    val connection = mock[Connection]
    (connection.setReadOnly _).expects(true)
    stubDriver.arm(connection)
    reconnect should be theSameInstanceAs connection
    stubDriver.lastUrl shouldBe expectedUrl
    stubDriver.lastProperties.getProperty("user") shouldBe "u"
    stubDriver.lastProperties.getProperty("password") shouldBe "p"
  }

  protected final def verifyTableListing(
      exec: SQLSourceOpExec with QueryAccess,
      tables: Seq[String]
  ): Unit = {
    val conn = mock[Connection]
    expectTableListing(conn, tables)
    exec.connection = conn
    exec.loadTables()
    exec.tableNames.toList shouldBe tables.toList
  }

  protected final def verifyOpen(exec: SQLSourceOpExec): Unit = {
    val conn = connectionForOpen(Seq("other", "tbl"))
    stubDriver.arm(conn)
    exec.open()
    exec.connection should be theSameInstanceAs conn
    exec.tableNames.toList shouldBe List("other", "tbl")
    exec.batchByAttribute shouldBe None
  }

  protected final def verifyMissingTable(exec: SQLSourceOpExec): Unit = {
    stubDriver.arm(connectionForOpen(Seq("other")))
    intercept[RuntimeException](exec.open()).getMessage shouldBe "Can't find the given table `tbl`."
  }

  protected final def keywordGuardCases(
      keyword: String,
      expectedClause: String,
      placeholderName: String
  )(newExec: (SQLSourceOpDesc => Unit) => SQLSourceOpExec with QueryAccess): Unit = {
    it should "add nothing when keyword search is switched off" in {
      val exec = newExec { desc =>
        desc.keywordSearch = Option(false)
        desc.keywordSearchByColumn = Option("name")
        desc.keywords = Option(keyword)
      }
      exec.filterClause shouldBe ""
    }

    it should "add nothing when no search column is configured" in {
      val exec = newExec { desc =>
        desc.keywordSearch = Option(true)
        desc.keywords = Option(keyword)
      }
      exec.filterClause shouldBe ""
    }

    it should s"still emit the $placeholderName placeholder when the keywords are missing" in {
      val exec = newExec { desc =>
        desc.keywordSearch = Option(true)
        desc.keywordSearchByColumn = Option("name")
      }
      // The query keeps the placeholder for None, while JDBC parameter binding skips it.
      exec.desc.keywords shouldBe None
      exec.filterClause shouldBe expectedClause
      exec.sqlQuery shouldBe Some(baseSelect + expectedClause + ";")
    }

    it should "refuse a keyword search on a non-string column" in {
      val exec = newExec { desc =>
        desc.keywordSearch = Option(true)
        desc.keywordSearchByColumn = Option("id")
        desc.keywords = Option("42")
      }
      intercept[RuntimeException](exec.filterClause).getMessage shouldBe
        "Can't do keyword search on type integer"
    }

    it should "refuse a keyword search on a column that is not in the schema" in {
      val exec = newExec { desc =>
        desc.keywordSearch = Option(true)
        desc.keywordSearchByColumn = Option("nope")
        desc.keywords = Option("x")
      }
      intercept[RuntimeException](exec.filterClause).getMessage shouldBe
        "nope is not contained in the schema"
    }
  }

  protected final def verifyRenderedQuery(
      exec: SQLSourceOpExec with QueryAccess,
      expectedClause: String
  ): Unit = {
    exec.sqlQuery shouldBe Some(baseSelect + expectedClause + ";")
    exec.curLimit = Some(3L)
    exec.curOffset = Some(2L)
    exec.sqlQuery shouldBe Some(baseSelect + expectedClause + " LIMIT ? OFFSET ?;")
  }

  protected final def verifyKeywordRows(
      exec: SQLSourceOpExec,
      expectedClause: String,
      keyword: String,
      expectedName: String
  ): Unit = {
    val conn = connectionForOpen(Seq("tbl"))
    val queryStatement = mock[PreparedStatement]
    val rows = mock[ResultSet]

    (conn
      .prepareStatement(_: String))
      .expects(baseSelect + expectedClause + ";")
      .returning(queryStatement)
    (queryStatement.setString _).expects(1, keyword)
    (queryStatement.executeQuery: () => ResultSet).expects().returning(rows)
    inSequence {
      (rows.next _).expects().returning(true)
      (rows.next _).expects().returning(false)
    }
    (rows.getObject(_: String)).expects("id").returning(Int.box(7))
    (rows.getObject(_: String)).expects("name").returning(expectedName)
    (rows.close _).expects()
    (queryStatement.close _).expects()

    stubDriver.arm(conn)
    exec.open()
    val tuples = exec.produceTuple().map(_.asInstanceOf[Tuple]).toList

    tuples should have size 1
    tuples.head.getField[Any]("id") shouldBe 7
    tuples.map(_.getField[Any]("name")) shouldBe List(expectedName)
  }

  protected final def verifyClose(exec: SQLSourceOpExec): Unit = {
    val conn = connectionForOpen(Seq("tbl"))
    (conn.close _).expects()
    stubDriver.arm(conn)
    exec.open()
    exec.close()
  }
}
