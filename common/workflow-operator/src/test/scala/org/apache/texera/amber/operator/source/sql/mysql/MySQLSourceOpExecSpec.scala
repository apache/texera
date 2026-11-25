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
import org.apache.texera.amber.operator.source.sql.{
  DialectBehavior,
  JdbcSourceOpExecTestHarness,
  SQLSourceOpExec
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, Types}
import scala.annotation.nowarn

// MySQLSourceOpExec is @deprecated (no longer executable) but retained so legacy workflows keep
// working; the coverage below pins the dialect-specific SQL it still produces.
@nowarn("cat=deprecation")
class MySQLSourceOpExecSpec
    extends AnyFlatSpec
    with Matchers
    with JdbcSourceOpExecTestHarness[MySQLSourceOpDesc] {

  override protected def jdbcUrlPrefix: String = "jdbc:mysql://"
  override protected def tableListingSql: String =
    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;"
  // The MySQL listing binds the database name, while PostgreSQL has no parameters.
  override protected def listingDatabase: Option[String] = Some("db")
  override protected def newDescriptor(): MySQLSourceOpDesc = new MySQLSourceOpDesc
  override protected def newDialectExec(descString: String): SQLSourceOpExec with QueryAccess =
    new MySQLSourceOpExec(descString) with QueryAccess {
      def fetchTableNamesSql: String = FETCH_TABLE_NAMES_SQL
      def establish(): Connection = establishConn()
      def loadTables(): Unit = loadTableNames()
      def sqlQuery: Option[String] = generateSqlQuery
      def filterClause: String = buildFilterClause(addFilterConditions)
    }

  registerDialectBehaviors(
    DialectBehavior(
      dialectLabel = "MySQL",
      jdbcUrl = "jdbc:mysql://127.0.0.1:1/db?autoReconnect=true&useSSL=true",
      filterClause = " AND MATCH(name) AGAINST (? IN BOOLEAN MODE)",
      placeholderName = "MATCH",
      keyword = "sore throat",
      expectedRowName = "a sore throat",
      extraCol = ("created", Types.TIMESTAMP, AttributeType.TIMESTAMP),
      listSummary = "list information_schema tables scoped to the database",
      emptyTablesNoun = "tables",
      listingLabel = "the MySQL listing",
      filterSummary = "emit MySQL's boolean-mode fulltext MATCH clause for a string column",
      produceVerb = "run the MATCH query"
    )
  )
}
