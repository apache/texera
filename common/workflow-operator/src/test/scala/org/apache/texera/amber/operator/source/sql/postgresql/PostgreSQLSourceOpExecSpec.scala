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
import org.apache.texera.amber.operator.source.sql.{
  DialectBehavior,
  JdbcSourceOpExecTestHarness,
  SQLSourceOpExec
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, Types}

class PostgreSQLSourceOpExecSpec
    extends AnyFlatSpec
    with Matchers
    with JdbcSourceOpExecTestHarness[PostgreSQLSourceOpDesc] {

  // Loopback port 1 is refused even when the real pgjdbc driver is first offered the URL, so
  // DriverManager falls through to stubDriver below.
  override protected def jdbcUrlPrefix: String = s"jdbc:postgresql://$testHost:$testPort/"
  override protected def tableListingSql: String =
    "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE';"
  override protected def newDescriptor(): PostgreSQLSourceOpDesc = new PostgreSQLSourceOpDesc
  override protected def newDialectExec(descString: String): SQLSourceOpExec with QueryAccess =
    new PostgreSQLSourceOpExec(descString) with QueryAccess {
      def fetchTableNamesSql: String = FETCH_TABLE_NAMES_SQL
      def establish(): Connection = establishConn()
      def loadTables(): Unit = loadTableNames()
      def sqlQuery: Option[String] = generateSqlQuery
      def filterClause: String = buildFilterClause(addFilterConditions)
    }

  registerDialectBehaviors(
    DialectBehavior(
      dialectLabel = "PostgreSQL",
      jdbcUrl = "jdbc:postgresql://127.0.0.1:1/db",
      filterClause = " AND name @@ to_tsquery(?)",
      placeholderName = "to_tsquery",
      keyword = "sore & throat",
      expectedRowName = null,
      extraCol = ("score", Types.DOUBLE, AttributeType.DOUBLE),
      listSummary = "list every base table without binding a database parameter",
      emptyTablesNoun = "base tables",
      listingLabel = "the listing",
      filterSummary = "emit a to_tsquery full-text clause for a string column",
      produceVerb = "run the to_tsquery statement",
      forbidClauseTerm = Some("MATCH")
    )
  )
}
