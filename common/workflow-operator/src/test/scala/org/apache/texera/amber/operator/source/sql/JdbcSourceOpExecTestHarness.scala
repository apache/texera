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
import scala.collection.mutable
import scala.reflect.ClassTag

/** What one dialect's executor spec expects. The summary, noun, label and verb fields carry the
  * wording each dialect's own case names used, so every case keeps its name.
  */
private[sql] final case class DialectBehavior(
    dialectLabel: String,
    jdbcUrl: String,
    filterClause: String,
    placeholderName: String,
    keyword: String,
    expectedRowName: String,
    extraCol: (String, Int, AttributeType),
    listSummary: String,
    emptyTablesNoun: String,
    listingLabel: String,
    filterSummary: String,
    produceVerb: String,
    forbidClauseTerm: Option[String] = None
)

private[sql] trait JdbcSourceOpExecTestHarness[D <: SQLSourceOpDesc]
    extends MockFactory
    with BeforeAndAfterAll {
  this: AnyFlatSpec with Matchers =>

  protected val testHost = "127.0.0.1"
  // Port 1 is never served: a loopback connection is refused instantly so DriverManager falls
  // through to stubDriver below even if a real dialect driver is present on the classpath.
  protected val testPort = "1"

  protected def jdbcUrlPrefix: String
  protected def tableListingSql: String
  protected def listingDatabase: Option[String] = None
  protected def newDescriptor(): D
  protected def newDialectExec(descString: String): SQLSourceOpExec with QueryAccess

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

  protected final def descJson(configure: D => Unit = _ => ()): String = {
    val desc = newDescriptor()
    desc.host = testHost
    desc.port = testPort
    desc.database = "db"
    desc.table = "tbl"
    desc.username = "u"
    desc.password = "p"
    configure(desc)
    objectMapper.writeValueAsString(desc)
  }

  protected final def keywordJson(
      kw: Option[String] = None,
      col: Option[String] = Some("name"),
      search: Boolean = true
  ): String =
    descJson { desc =>
      desc.keywordSearch = Option(search)
      col.foreach(c => desc.keywordSearchByColumn = Option(c))
      kw.foreach(k => desc.keywords = Option(k))
    }

  protected final def newExec(
      json: String = descJson(),
      columns: Seq[(String, Int)] = Seq("id" -> Types.INTEGER, "name" -> Types.VARCHAR)
  ): SQLSourceOpExec with QueryAccess = {
    stubDriver.arm(schemaConn(columns))
    newDialectExec(json)
  }

  private def connectionForOpen(tables: Seq[String]): Connection = {
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true)
    expectTableListing(conn, tables)
    conn
  }

  protected val baseSelect = "\nSELECT * FROM tbl where 1 = 1"

  protected trait QueryAccess {
    def fetchTableNamesSql: String
    def establish(): Connection
    def loadTables(): Unit
    def filterClause: String
    def sqlQuery: Option[String]
    protected final def buildFilterClause(append: StringBuilder => Unit): String = {
      val queryBuilder = new StringBuilder
      append(queryBuilder)
      queryBuilder.result()
    }
  }

  protected final def registerDialectBehaviors(
      d: DialectBehavior
  )(implicit descTag: ClassTag[D]): Unit = {
    val execName = s"${d.dialectLabel}SourceOpExec"
    val descName = s"${d.dialectLabel}SourceOpDesc"
    val kwJson = keywordJson(kw = Some(d.keyword))

    execName should s"deserialize a $descName and derive the schema from JDBC metadata" in {
      val (extraName, extraJdbcType, extraAttrType) = d.extraCol
      val exec = newExec(columns =
        Seq("id" -> Types.INTEGER, "name" -> Types.VARCHAR, extraName -> extraJdbcType)
      )
      exec.desc shouldBe a[D]
      exec.desc.table shouldBe "tbl"
      exec.desc.database shouldBe "db"
      exec.schema.getAttributeNames shouldBe List("id", "name", extraName)
      exec.schema.getAttribute("id").getType shouldBe AttributeType.INTEGER
      exec.schema.getAttribute("name").getType shouldBe AttributeType.STRING
      exec.schema.getAttribute(extraName).getType shouldBe extraAttrType
      exec.fetchTableNamesSql shouldBe tableListingSql
      // A listing scoped to no database (PostgreSQL's) binds no parameter.
      if (listingDatabase.isEmpty) exec.fetchTableNamesSql should not include "?"
    }

    s"$execName.establishConn" should s"dial the ${d.dialectLabel} JDBC URL built from the descriptor" in {
      val exec = newExec()
      stubDriver.lastUrl shouldBe d.jdbcUrl
      val connection = mock[Connection]
      (connection.setReadOnly _).expects(true)
      stubDriver.arm(connection)
      exec.establish() should be theSameInstanceAs connection
      stubDriver.lastUrl shouldBe d.jdbcUrl
      stubDriver.lastProperties.getProperty("user") shouldBe "u"
      stubDriver.lastProperties.getProperty("password") shouldBe "p"
    }

    it should "propagate the SQLException when the server refuses the connection" in {
      val exec = newExec()
      // nothing armed: the driver reports the failure and the executor must not swallow it
      a[SQLException] should be thrownBy exec.establish()
    }

    def checkTableListing(tables: Seq[String]): Unit = {
      val exec = newExec()
      val conn = mock[Connection]
      expectTableListing(conn, tables)
      exec.connection = conn
      exec.loadTables()
      exec.tableNames.toList shouldBe tables.toList
    }

    s"$execName.loadTableNames" should d.listSummary in {
      checkTableListing(Seq("tbl", "other"))
    }

    it should s"leave the table list empty when the database exposes no ${d.emptyTablesNoun}" in {
      checkTableListing(Seq.empty)
    }

    s"$execName.open" should s"connect and accept a table present in ${d.listingLabel}" in {
      val exec = newExec()
      val conn = connectionForOpen(Seq("other", "tbl"))
      stubDriver.arm(conn)
      exec.open()
      exec.connection should be theSameInstanceAs conn
      exec.tableNames.toList shouldBe List("other", "tbl")
      exec.batchByAttribute shouldBe None
    }

    it should s"reject a table missing from ${d.listingLabel}" in {
      val exec = newExec()
      stubDriver.arm(connectionForOpen(Seq("other")))
      intercept[RuntimeException](exec.open()).getMessage shouldBe
        "Can't find the given table `tbl`."
    }

    s"$execName.addFilterConditions" should d.filterSummary in {
      val exec = newExec(kwJson)
      exec.filterClause shouldBe d.filterClause
      // the keyword itself stays a bind parameter, only the column name is spliced in
      exec.filterClause should not include d.keyword
      d.forbidClauseTerm.foreach(exec.filterClause should not include _)
    }

    it should "add nothing when keyword search is switched off" in {
      newExec(keywordJson(kw = Some(d.keyword), search = false)).filterClause shouldBe ""
    }

    it should "add nothing when no search column is configured" in {
      newExec(keywordJson(kw = Some(d.keyword), col = None)).filterClause shouldBe ""
    }

    it should s"still emit the ${d.placeholderName} placeholder when the keywords are missing" in {
      val exec = newExec(keywordJson())
      // The query keeps the placeholder for None, while JDBC parameter binding skips it.
      exec.desc.keywords shouldBe None
      exec.filterClause shouldBe d.filterClause
      exec.sqlQuery shouldBe Some(baseSelect + d.filterClause + ";")
    }

    def checkFilterError(kw: String, col: String, expectedMsg: String): Unit = {
      val exec = newExec(keywordJson(kw = Some(kw), col = Some(col)))
      intercept[RuntimeException](exec.filterClause).getMessage shouldBe expectedMsg
    }

    it should "refuse a keyword search on a non-string column" in {
      checkFilterError("42", "id", "Can't do keyword search on type integer")
    }

    it should "refuse a keyword search on a column that is not in the schema" in {
      checkFilterError("x", "nope", "nope is not contained in the schema")
    }

    s"$execName.generateSqlQuery" should
      s"splice the ${d.placeholderName} clause into the full statement" in {
      val exec = newExec(kwJson)
      exec.sqlQuery shouldBe Some(baseSelect + d.filterClause + ";")
      exec.curLimit = Some(3L)
      exec.curOffset = Some(2L)
      exec.sqlQuery shouldBe Some(baseSelect + d.filterClause + " LIMIT ? OFFSET ?;")
    }

    it should "produce a plain statement when no keyword search is configured" in {
      newExec().sqlQuery shouldBe Some(baseSelect + ";")
    }

    s"$execName.produceTuple" should s"${d.produceVerb} and bind the keyword parameter" in {
      val exec = newExec(kwJson)
      val conn = connectionForOpen(Seq("tbl"))
      val queryStatement = mock[PreparedStatement]
      val rows = mock[ResultSet]
      (conn
        .prepareStatement(_: String))
        .expects(baseSelect + d.filterClause + ";")
        .returning(queryStatement)
      (queryStatement.setString _).expects(1, d.keyword)
      (queryStatement.executeQuery: () => ResultSet).expects().returning(rows)
      inSequence {
        (rows.next _).expects().returning(true)
        (rows.next _).expects().returning(false)
      }
      (rows.getObject(_: String)).expects("id").returning(Int.box(7))
      (rows.getObject(_: String)).expects("name").returning(d.expectedRowName)
      (rows.close _).expects()
      (queryStatement.close _).expects()

      stubDriver.arm(conn)
      exec.open()
      val tuples = exec.produceTuple().map(_.asInstanceOf[Tuple]).toList
      tuples should have size 1
      tuples.head.getField[Any]("id") shouldBe 7
      tuples.map(_.getField[Any]("name")) shouldBe List(d.expectedRowName)
    }

    s"$execName.close" should "close the connection established by open" in {
      val exec = newExec()
      val conn = connectionForOpen(Seq("tbl"))
      (conn.close _).expects()
      stubDriver.arm(conn)
      exec.open()
      exec.close()
    }
  }
}
