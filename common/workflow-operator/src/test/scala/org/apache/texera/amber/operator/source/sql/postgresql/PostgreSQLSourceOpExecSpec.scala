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

package org.apache.texera.amber.operator.source.sql.postgresql

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.source.sql.JdbcSourceOpExecTestHarness
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{SQLException, Types}

class PostgreSQLSourceOpExecSpec
    extends AnyFlatSpec
    with Matchers
    with JdbcSourceOpExecTestHarness {

  private val HOST = "127.0.0.1"
  // Port 1 is never served. The real pgjdbc driver is on the test classpath and is offered the URL
  // first, but a loopback connection to port 1 is refused instantly and DriverManager then falls
  // through to the stub driver below.
  private val PORT = "1"
  private val JDBC_URL = "jdbc:postgresql://127.0.0.1:1/db"
  private val FETCH_TABLES =
    "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE';"
  private val TSQUERY_CLAUSE = " AND name @@ to_tsquery(?)"

  override protected def jdbcUrlPrefix: String = "jdbc:postgresql://" + HOST + ":" + PORT + "/"
  override protected def tableListingSql: String = FETCH_TABLES

  /** Re-exposes the protected query-building surface of the dialect executor. */
  private class TestPostgreSQLSourceOpExec(descString: String)
      extends PostgreSQLSourceOpExec(descString)
      with QueryAccess {
    def fetchTableNamesSql: String = FETCH_TABLE_NAMES_SQL
    def loadTables(): Unit = loadTableNames()
    def sqlQuery: Option[String] = generateSqlQuery
    def filterClause: String = {
      val queryBuilder = new StringBuilder
      addFilterConditions(queryBuilder)
      queryBuilder.result()
    }
  }

  private def descJson(configure: PostgreSQLSourceOpDesc => Unit = _ => ()): String =
    configuredDescriptorJson(new PostgreSQLSourceOpDesc, HOST, PORT)(configure)

  private val keywordJson = descJson { desc =>
    desc.keywordSearch = Option(true)
    desc.keywordSearchByColumn = Option("name")
    desc.keywords = Option("sore & throat")
  }

  private def newExec(
      json: String = descJson(),
      columns: Seq[(String, Int)] = Seq("id" -> Types.INTEGER, "name" -> Types.VARCHAR)
  ): TestPostgreSQLSourceOpExec = {
    stubDriver.arm(schemaConn(columns))
    new TestPostgreSQLSourceOpExec(json)
  }

  "PostgreSQLSourceOpExec" should
    "deserialize a PostgreSQLSourceOpDesc and derive the schema from JDBC metadata" in {
    val exec = newExec(columns =
      Seq("id" -> Types.INTEGER, "name" -> Types.VARCHAR, "score" -> Types.DOUBLE)
    )

    exec.desc shouldBe a[PostgreSQLSourceOpDesc]
    exec.desc.table shouldBe "tbl"
    exec.desc.database shouldBe "db"
    exec.schema.getAttributeNames shouldBe List("id", "name", "score")
    exec.schema.getAttribute("id").getType shouldBe AttributeType.INTEGER
    exec.schema.getAttribute("name").getType shouldBe AttributeType.STRING
    exec.schema.getAttribute("score").getType shouldBe AttributeType.DOUBLE
    // PostgreSQL filters by table type instead of scoping to a database, so no bind parameter
    exec.fetchTableNamesSql shouldBe FETCH_TABLES
    exec.fetchTableNamesSql should not include "?"
  }

  "PostgreSQLSourceOpExec.establishConn" should
    "dial the PostgreSQL JDBC URL built from the descriptor" in {
    val exec = newExec()
    verifyReconnect(JDBC_URL)(exec.establishConn())
  }

  it should "propagate the SQLException when the server refuses the connection" in {
    val exec = newExec()
    // nothing armed: the driver reports the failure and the executor must not swallow it
    a[SQLException] should be thrownBy exec.establishConn()
  }

  "PostgreSQLSourceOpExec.loadTableNames" should
    "list every base table without binding a database parameter" in {
    verifyTableListing(newExec(), Seq("tbl", "other"))
  }

  it should "leave the table list empty when the database exposes no base tables" in {
    verifyTableListing(newExec(), Seq.empty)
  }

  "PostgreSQLSourceOpExec.open" should "connect and accept a table present in the listing" in {
    verifyOpen(newExec())
  }

  it should "reject a table missing from the listing" in {
    verifyMissingTable(newExec())
  }

  "PostgreSQLSourceOpExec.addFilterConditions" should
    "emit a to_tsquery full-text clause for a string column" in {
    val exec = newExec(keywordJson)
    exec.filterClause shouldBe TSQUERY_CLAUSE
    // the keyword itself stays a bind parameter, only the column name is spliced in
    exec.filterClause should not include "sore & throat"
    // and it must not borrow MySQL's dialect
    exec.filterClause should not include "MATCH"
  }

  keywordGuardCases("sore & throat", TSQUERY_CLAUSE, "to_tsquery")(configure =>
    newExec(descJson(configure))
  )

  "PostgreSQLSourceOpExec.generateSqlQuery" should
    "splice the to_tsquery clause into the full statement" in {
    verifyRenderedQuery(newExec(keywordJson), TSQUERY_CLAUSE)
  }

  it should "produce a plain statement when no keyword search is configured" in {
    newExec().sqlQuery shouldBe Some(baseSelect + ";")
  }

  "PostgreSQLSourceOpExec.produceTuple" should
    "run the to_tsquery statement and bind the keyword parameter" in {
    verifyKeywordRows(newExec(keywordJson), TSQUERY_CLAUSE, "sore & throat", null)
  }

  "PostgreSQLSourceOpExec.close" should "close the connection established by open" in {
    verifyClose(newExec())
  }
}
