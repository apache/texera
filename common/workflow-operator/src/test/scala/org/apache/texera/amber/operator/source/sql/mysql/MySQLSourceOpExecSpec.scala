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

import org.apache.texera.amber.core.tuple.{AttributeType, Tuple}
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
  SQLFeatureNotSupportedException,
  Types
}
import java.util.Properties
import java.util.logging.Logger
import scala.annotation.nowarn
import scala.collection.mutable

// MySQLSourceOpExec is @deprecated (no longer executable) but retained so legacy workflows keep
// working; the coverage below pins the dialect-specific SQL it still produces.
@nowarn("cat=deprecation")
class MySQLSourceOpExecSpec
    extends AnyFlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll {

  private val HOST = "127.0.0.1"
  // Port 1 is never served: no MySQL driver ships on this classpath, and if one ever did it would
  // be refused instantly and DriverManager would fall through to the stub driver below.
  private val PORT = "1"
  private val JDBC_URL = "jdbc:mysql://127.0.0.1:1/db?autoReconnect=true&useSSL=true"
  private val BASE = "\nSELECT * FROM tbl where 1 = 1"
  private val FETCH_TABLES =
    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;"
  private val MATCH_CLAUSE = " AND MATCH(name) AGAINST (? IN BOOLEAN MODE)"

  /**
    * A JDBC driver that hands out pre-armed connections for `jdbc:mysql://` URLs. It lets the
    * executor be built through the real `MySQLConnUtil` code path without a MySQL server.
    */
  private class StubDriver extends Driver {
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

    override def acceptsURL(url: String): Boolean = url != null && url.startsWith("jdbc:mysql://")
    override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
      Array.empty
    override def getMajorVersion: Int = 1
    override def getMinorVersion: Int = 0
    override def jdbcCompliant(): Boolean = false
    override def getParentLogger: Logger = throw new SQLFeatureNotSupportedException()
  }

  private val stubDriver = new StubDriver

  override def beforeAll(): Unit = {
    super.beforeAll()
    DriverManager.registerDriver(stubDriver)
  }

  override def afterAll(): Unit = {
    DriverManager.deregisterDriver(stubDriver)
    super.afterAll()
  }

  /** Re-exposes the protected query-building surface of the dialect executor. */
  private class TestMySQLSourceOpExec(descString: String) extends MySQLSourceOpExec(descString) {
    def fetchTableNamesSql: String = FETCH_TABLE_NAMES_SQL
    def loadTables(): Unit = loadTableNames()
    def sqlQuery: Option[String] = generateSqlQuery
    def filterClause: String = {
      val queryBuilder = new StringBuilder
      addFilterConditions(queryBuilder)
      queryBuilder.result()
    }
  }

  private def descJson(configure: MySQLSourceOpDesc => Unit = _ => ()): String = {
    val desc = new MySQLSourceOpDesc
    desc.host = HOST
    desc.port = PORT
    desc.database = "db"
    desc.table = "tbl"
    desc.username = "u"
    desc.password = "p"
    configure(desc)
    objectMapper.writeValueAsString(desc)
  }

  private val keywordJson = descJson { desc =>
    desc.keywordSearch = Option(true)
    desc.keywordSearchByColumn = Option("name")
    desc.keywords = Option("sore throat")
  }

  /** A connection answering the `sourceSchema()` JDBC-metadata probe run in the constructor. */
  private def schemaConn(columns: Seq[(String, Int)]): Connection = {
    val conn = mock[Connection]
    val metaData = mock[DatabaseMetaData]
    val columnsRs = mock[ResultSet]
    // once inside MySQLConnUtil.connect, once inside SQLSourceOpDesc.querySchema
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

  private def newExec(
      json: String = descJson(),
      columns: Seq[(String, Int)] = Seq("id" -> Types.INTEGER, "name" -> Types.VARCHAR)
  ): TestMySQLSourceOpExec = {
    stubDriver.arm(schemaConn(columns))
    new TestMySQLSourceOpExec(json)
  }

  /** Stubs the `information_schema` probe that `loadTableNames` issues. */
  private def expectTableListing(conn: Connection, tables: Seq[String]): Unit = {
    val statement = mock[PreparedStatement]
    val resultSet = mock[ResultSet]
    (conn.prepareStatement(_: String)).expects(FETCH_TABLES).returning(statement)
    // MySQL scopes the listing to the configured database via a bound parameter
    (statement.setString _).expects(1, "db")
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

  "MySQLSourceOpExec" should "deserialize a MySQLSourceOpDesc and derive the schema from JDBC metadata" in {
    val exec = newExec(columns =
      Seq("id" -> Types.INTEGER, "name" -> Types.VARCHAR, "created" -> Types.TIMESTAMP)
    )

    exec.desc shouldBe a[MySQLSourceOpDesc]
    exec.desc.table shouldBe "tbl"
    exec.desc.database shouldBe "db"
    exec.schema.getAttributeNames shouldBe List("id", "name", "created")
    exec.schema.getAttribute("id").getType shouldBe AttributeType.INTEGER
    exec.schema.getAttribute("name").getType shouldBe AttributeType.STRING
    exec.schema.getAttribute("created").getType shouldBe AttributeType.TIMESTAMP
    exec.fetchTableNamesSql shouldBe FETCH_TABLES
  }

  "MySQLSourceOpExec.establishConn" should "dial the MySQL JDBC URL built from the descriptor" in {
    val exec = newExec()
    stubDriver.lastUrl shouldBe JDBC_URL

    val reconnected = mock[Connection]
    // MySQLConnUtil.connect flips the fresh connection to read-only
    (reconnected.setReadOnly _).expects(true)
    stubDriver.arm(reconnected)

    exec.establishConn() should be theSameInstanceAs reconnected
    stubDriver.lastUrl shouldBe JDBC_URL
    stubDriver.lastProperties.getProperty("user") shouldBe "u"
    stubDriver.lastProperties.getProperty("password") shouldBe "p"
  }

  it should "propagate the SQLException when the server refuses the connection" in {
    val exec = newExec()
    // nothing armed: the driver reports the failure and the executor must not swallow it
    a[SQLException] should be thrownBy exec.establishConn()
  }

  "MySQLSourceOpExec.loadTableNames" should "list information_schema tables scoped to the database" in {
    val exec = newExec()
    val conn = mock[Connection]
    expectTableListing(conn, Seq("tbl", "other"))

    exec.connection = conn
    exec.loadTables()

    exec.tableNames.toList shouldBe List("tbl", "other")
  }

  it should "leave the table list empty when the database exposes no tables" in {
    val exec = newExec()
    val conn = mock[Connection]
    expectTableListing(conn, Seq.empty)

    exec.connection = conn
    exec.loadTables()

    exec.tableNames shouldBe empty
  }

  "MySQLSourceOpExec.open" should "connect and accept a table present in the MySQL listing" in {
    val exec = newExec()
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true)
    expectTableListing(conn, Seq("other", "tbl"))
    stubDriver.arm(conn)

    exec.open()

    exec.connection should be theSameInstanceAs conn
    exec.tableNames.toList shouldBe List("other", "tbl")
    exec.batchByAttribute shouldBe None
  }

  it should "reject a table missing from the MySQL listing" in {
    val exec = newExec()
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true)
    expectTableListing(conn, Seq("other"))
    stubDriver.arm(conn)

    intercept[RuntimeException](exec.open()).getMessage shouldBe "Can't find the given table `tbl`."
  }

  "MySQLSourceOpExec.addFilterConditions" should
    "emit MySQL's boolean-mode fulltext MATCH clause for a string column" in {
    val exec = newExec(keywordJson)
    exec.filterClause shouldBe MATCH_CLAUSE
    // the keyword itself stays a bind parameter, only the column name is spliced in
    exec.filterClause should not include "sore throat"
  }

  it should "add nothing when keyword search is switched off" in {
    val exec = newExec(descJson { desc =>
      desc.keywordSearch = Option(false)
      desc.keywordSearchByColumn = Option("name")
      desc.keywords = Option("sore throat")
    })
    exec.filterClause shouldBe ""
  }

  it should "add nothing when no search column is configured" in {
    val exec = newExec(descJson { desc =>
      desc.keywordSearch = Option(true)
      desc.keywords = Option("sore throat")
    })
    exec.filterClause shouldBe ""
  }

  it should "still emit the MATCH placeholder when the keywords are missing" in {
    val exec = newExec(descJson { desc =>
      desc.keywordSearch = Option(true)
      desc.keywordSearchByColumn = Option("name")
    })
    // The guard reads `desc.keywords != null`, but `keywords` is an Option, so it is never null and
    // the empty case slips through. Pin the behavior as it stands: the clause keeps its bind
    // placeholder even though SQLSourceOpExec.getNextQuery (which unwraps with `.orNull`) binds
    // nothing for it, so the statement would fail at execution time.
    exec.desc.keywords shouldBe None
    exec.filterClause shouldBe MATCH_CLAUSE
    exec.sqlQuery shouldBe Some(BASE + MATCH_CLAUSE + ";")
  }

  it should "refuse a keyword search on a non-string column" in {
    val exec = newExec(descJson { desc =>
      desc.keywordSearch = Option(true)
      desc.keywordSearchByColumn = Option("id")
      desc.keywords = Option("42")
    })
    intercept[RuntimeException](exec.filterClause).getMessage shouldBe
      "Can't do keyword search on type integer"
  }

  it should "refuse a keyword search on a column that is not in the schema" in {
    val exec = newExec(descJson { desc =>
      desc.keywordSearch = Option(true)
      desc.keywordSearchByColumn = Option("nope")
      desc.keywords = Option("x")
    })
    intercept[RuntimeException](exec.filterClause).getMessage shouldBe
      "nope is not contained in the schema"
  }

  "MySQLSourceOpExec.generateSqlQuery" should "splice the MATCH clause into the full statement" in {
    val exec = newExec(keywordJson)
    exec.sqlQuery shouldBe Some(BASE + MATCH_CLAUSE + ";")

    exec.curLimit = Some(3L)
    exec.curOffset = Some(2L)
    exec.sqlQuery shouldBe Some(BASE + MATCH_CLAUSE + " LIMIT ? OFFSET ?;")
  }

  it should "produce a plain statement when no keyword search is configured" in {
    newExec().sqlQuery shouldBe Some(BASE + ";")
  }

  "MySQLSourceOpExec.produceTuple" should "run the MATCH query and bind the keyword parameter" in {
    val exec = newExec(keywordJson)
    val conn = mock[Connection]
    val queryStatement = mock[PreparedStatement]
    val rows = mock[ResultSet]

    (conn.setReadOnly _).expects(true)
    expectTableListing(conn, Seq("tbl"))

    (conn
      .prepareStatement(_: String))
      .expects(BASE + MATCH_CLAUSE + ";")
      .returning(queryStatement)
    (queryStatement.setString _).expects(1, "sore throat")
    (queryStatement.executeQuery: () => ResultSet).expects().returning(rows)
    inSequence {
      (rows.next _).expects().returning(true)
      (rows.next _).expects().returning(false)
    }
    (rows.getObject(_: String)).expects("id").returning(Int.box(7))
    (rows.getObject(_: String)).expects("name").returning("a sore throat")
    (rows.close _).expects()
    (queryStatement.close _).expects()

    stubDriver.arm(conn)
    exec.open()
    val tuples = exec.produceTuple().map(_.asInstanceOf[Tuple]).toList

    tuples should have size 1
    tuples.head.getField[Any]("id") shouldBe 7
    tuples.head.getField[Any]("name") shouldBe "a sore throat"
  }

  "MySQLSourceOpExec.close" should "close the connection established by open" in {
    val exec = newExec()
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true)
    expectTableListing(conn, Seq("tbl"))
    (conn.close _).expects()
    stubDriver.arm(conn)

    exec.open()
    exec.close()
  }
}
