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

import io.vertx.core.Vertx
import io.vertx.mysqlclient.{MySQLBuilder, MySQLConnectOptions, SslMode}
import io.vertx.sqlclient.{Pool, PoolOptions}

object MySQLConnUtil {

  // Mirrors the old JDBC URL `useSSL=true`: prefer SSL, do not fail if the
  // server refuses or presents an untrusted certificate.
  def createPool(
      vertx: Vertx,
      host: String,
      port: String,
      database: String,
      username: String,
      password: String
  ): Pool = {
    val connectOptions = new MySQLConnectOptions()
      .setHost(host)
      .setPort(port.toInt)
      .setDatabase(database)
      .setUser(username)
      .setPassword(password)
      .setSslMode(SslMode.PREFERRED)
      .setTrustAll(true)
      .setReconnectAttempts(2)
    MySQLBuilder
      .pool()
      .`with`(new PoolOptions().setMaxSize(1))
      .connectingTo(connectOptions)
      .using(vertx)
      .build()
  }
}
