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

package edu.uci.ics.texera.dao

import org.jooq.impl.DSL
import org.jooq.{DSLContext, SQLDialect}
import org.postgresql.ds.PGSimpleDataSource

/**
  * SqlServer class that manages a connection to a MySQL database using jOOQ.
  *
  * @param url The database connection URL, specifying the MySQL server and database name.
  * @param user The username for authenticating with the MySQL database.
  * @param password The password for authenticating with the MySQL database.
  */
class SqlServer private (url: String, user: String, password: String) {
  val SQL_DIALECT: SQLDialect = SQLDialect.POSTGRES
  private val dataSource: PGSimpleDataSource = new PGSimpleDataSource()
  var context: DSLContext = {
    dataSource.setUrl(url)
    dataSource.setUser(user)
    dataSource.setPassword(password)
    dataSource.setConnectTimeout(5)
    DSL.using(dataSource, SQL_DIALECT)
  }

  def createDSLContext(): DSLContext = context

  def replaceDSLContext(newContext: DSLContext): Unit = {
    context = newContext
  }
}

object SqlServer {
  private var instance: Option[SqlServer] = None

  def initConnection(url: String, user: String, password: String): Unit = {
    if (instance.isEmpty) {
      val server = new SqlServer(url, user, password)
      instance = Some(server)
    }
  }

  def getInstance(): SqlServer = {
    instance.get
  }

  /**
    * A utility function for create a transaction block using given sql context
    * @param dsl the sql context
    * @param block the code block to execute within the transaction
    * @tparam T the value will be returned by the code block
    * @return
    */
  def withTransaction[T](dsl: DSLContext)(block: DSLContext => T): T = {
    var result: Option[T] = None

    dsl.transaction(configuration => {
      val ctx = DSL.using(configuration)
      result = Some(block(ctx))
    })

    result.getOrElse(throw new RuntimeException("Transaction failed without result!"))
  }
}
