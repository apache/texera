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

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.source.sql.JdbcSourceOpExecTestHarness
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{SQLException, Types}
import scala.annotation.nowarn

// MySQLSourceOpExec is @deprecated (no longer executable) but retained so legacy workflows keep
// working; the coverage below pins the dialect-specific SQL it still produces.
@nowarn("cat=deprecation")
class MySQLSourceOpExecSpec extends AnyFlatSpec with Matchers with JdbcSourceOpExecTestHarness {

  private val HOST = "127.0.0.1"
  // Port 1 is never served: no MySQL driver ships on this classpath, and if one ever did it would
  // be refused instantly and DriverManager would fall through to the stub driver below.
  private val PORT = "1"
  private val JDBC_URL = "jdbc:mysql://127.0.0.1:1/db?autoReconnect=true&useSSL=true"
  private val FETCH_TABLES =
    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;"
  private val MATCH_CLAUSE = " AND MATCH(name) AGAINST (? IN BOOLEAN MODE)"

  override protected def jdbcUrlPrefix: String = "jdbc:mysql://"
  override protected def tableListingSql: String = FETCH_TABLES
  // The MySQL listing binds the database name, while PostgreSQL has no parameters.
  override protected def listingDatabase: Option[String] = Some("db")

  /** Re-exposes the protected query-building surface of the dialect executor. */
  private class TestMySQLSourceOpExec(descString: String)
      extends MySQLSourceOpExec(descString)
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

  private def descJson(configure: MySQLSourceOpDesc => Unit = _ => ()): String =
    configuredDescriptorJson(new MySQLSourceOpDesc, HOST, PORT)(configure)

  private val keywordJson = descJson { desc =>
    desc.keywordSearch = Option(true)
    desc.keywordSearchByColumn = Option("name")
    desc.keywords = Option("sore throat")
  }

  private def newExec(
      json: String = descJson(),
      columns: Seq[(String, Int)] = Seq("id" -> Types.INTEGER, "name" -> Types.VARCHAR)
  ): TestMySQLSourceOpExec = {
    stubDriver.arm(schemaConn(columns))
    new TestMySQLSourceOpExec(json)
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
    verifyReconnect(JDBC_URL)(exec.establishConn())
  }

  it should "propagate the SQLException when the server refuses the connection" in {
    val exec = newExec()
    // nothing armed: the driver reports the failure and the executor must not swallow it
    a[SQLException] should be thrownBy exec.establishConn()
  }

  "MySQLSourceOpExec.loadTableNames" should "list information_schema tables scoped to the database" in {
    verifyTableListing(newExec(), Seq("tbl", "other"))
  }

  it should "leave the table list empty when the database exposes no tables" in {
    verifyTableListing(newExec(), Seq.empty)
  }

  "MySQLSourceOpExec.open" should "connect and accept a table present in the MySQL listing" in {
    verifyOpen(newExec())
  }

  it should "reject a table missing from the MySQL listing" in {
    verifyMissingTable(newExec())
  }

  "MySQLSourceOpExec.addFilterConditions" should
    "emit MySQL's boolean-mode fulltext MATCH clause for a string column" in {
    val exec = newExec(keywordJson)
    exec.filterClause shouldBe MATCH_CLAUSE
    // the keyword itself stays a bind parameter, only the column name is spliced in
    exec.filterClause should not include "sore throat"
  }

  keywordGuardCases("sore throat", MATCH_CLAUSE, "MATCH")(configure => newExec(descJson(configure)))

  "MySQLSourceOpExec.generateSqlQuery" should "splice the MATCH clause into the full statement" in {
    verifyRenderedQuery(newExec(keywordJson), MATCH_CLAUSE)
  }

  it should "produce a plain statement when no keyword search is configured" in {
    newExec().sqlQuery shouldBe Some(baseSelect + ";")
  }

  "MySQLSourceOpExec.produceTuple" should "run the MATCH query and bind the keyword parameter" in {
    verifyKeywordRows(newExec(keywordJson), MATCH_CLAUSE, "sore throat", "a sore throat")
  }

  "MySQLSourceOpExec.close" should "close the connection established by open" in {
    verifyClose(newExec())
  }
}
