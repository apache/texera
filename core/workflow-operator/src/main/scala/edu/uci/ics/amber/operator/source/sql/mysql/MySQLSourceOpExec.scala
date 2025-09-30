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

package edu.uci.ics.amber.operator.source.sql.mysql

import edu.uci.ics.amber.core.tuple.AttributeType
import edu.uci.ics.amber.operator.source.sql.SQLSourceOpExec
import edu.uci.ics.amber.operator.source.sql.mysql.MySQLConnUtil.connect
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.sql._

class MySQLSourceOpExec private[mysql] (
    descString: String
) extends SQLSourceOpExec(descString) {
  override val desc: MySQLSourceOpDesc =
    objectMapper.readValue(descString, classOf[MySQLSourceOpDesc])
  schema = desc.sourceSchema()
  val FETCH_TABLE_NAMES_SQL =
    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;"

  @throws[SQLException]
  override def establishConn(): Connection =
    connect(desc.host, desc.port, desc.database, desc.username, desc.password)

  @throws[RuntimeException]
  override def addFilterConditions(queryBuilder: StringBuilder): Unit = {
    val keywordSearchByColumn = desc.keywordSearchByColumn.orNull
    if (
      desc.keywordSearch.getOrElse(false) && keywordSearchByColumn != null && desc.keywords != null
    ) {
      val columnType = schema.getAttribute(keywordSearchByColumn).getType

      if (columnType == AttributeType.STRING)
        // in sql prepared statement, column name cannot be inserted using PreparedStatement.setString either
        queryBuilder ++= " AND MATCH(" + keywordSearchByColumn + ") AGAINST (? IN BOOLEAN MODE)"
      else
        throw new RuntimeException("Can't do keyword search on type " + columnType.toString)
    }
  }

  @throws[SQLException]
  override protected def loadTableNames(): Unit = {
    val preparedStatement = connection.prepareStatement(FETCH_TABLE_NAMES_SQL)
    preparedStatement.setString(1, desc.database)
    val resultSet = preparedStatement.executeQuery
    while ({
      resultSet.next
    }) {
      tableNames += resultSet.getString(1)
    }
    resultSet.close()
    preparedStatement.close()
  }
}
