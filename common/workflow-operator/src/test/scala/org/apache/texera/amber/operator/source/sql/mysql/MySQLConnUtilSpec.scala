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

import org.scalatest.flatspec.AnyFlatSpec

import java.sql.SQLException

class MySQLConnUtilSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Strategy — same approach as PostgreSQLConnUtilSpec. The workflow-operator
  // test classpath does not carry a MySQL driver, so DriverManager.getConnection
  // throws SQLException("No suitable driver found for " + url). The URL is
  // present in the exception message, which is what we pin on.
  // ---------------------------------------------------------------------------

  private def expectFailureUrl(host: String, port: String, database: String): String = {
    val ex = intercept[SQLException] {
      MySQLConnUtil.connect(host, port, database, "u", "p")
    }
    ex.getMessage
  }

  // ---------------------------------------------------------------------------
  // URL composition — host/port/database
  // ---------------------------------------------------------------------------

  "MySQLConnUtil.connect" should
    "build a JDBC URL of the form jdbc:mysql://{host}:{port}/{database}?…" in {
    val msg = expectFailureUrl("host-m", "3306", "db-m")
    assert(
      msg.contains("jdbc:mysql://host-m:3306/db-m"),
      s"expected jdbc:mysql://host-m:3306/db-m in exception message, got: $msg"
    )
  }

  it should "interpolate distinct host/port/database values into the URL" in {
    val msg = expectFailureUrl("other-host", "33060", "other-db")
    assert(msg.contains("jdbc:mysql://other-host:33060/other-db"))
  }

  it should "place host BEFORE port" in {
    val msg = expectFailureUrl("a", "1", "x")
    assert(msg.contains("//a:1/"))
    assert(!msg.contains("//1:a/"))
  }

  // ---------------------------------------------------------------------------
  // Query parameters — autoReconnect=true and useSSL=true must be present
  // ---------------------------------------------------------------------------
  //
  // The MySQL URL format is `jdbc:mysql://{host}:{port}/{database}?
  // autoReconnect=true&useSSL=true`. A regression that dropped useSSL=true
  // would silently downgrade the connection's security; a regression that
  // dropped autoReconnect=true would silently change retry behavior. Pin
  // both query parameters explicitly.

  it should "include the `autoReconnect=true` query parameter" in {
    val msg = expectFailureUrl("h", "3306", "db")
    assert(
      msg.contains("autoReconnect=true"),
      s"URL must include autoReconnect=true, got: $msg"
    )
  }

  it should "include the `useSSL=true` query parameter (TLS contract)" in {
    val msg = expectFailureUrl("h", "3306", "db")
    assert(
      msg.contains("useSSL=true"),
      s"URL must include useSSL=true (TLS), got: $msg"
    )
  }

  it should "use `?` to separate the path from query and `&` between params" in {
    val msg = expectFailureUrl("h", "3306", "db")
    // The path ends at `/db`; everything after `?` is query params.
    // Pin the canonical "jdbc:mysql://h:3306/db?autoReconnect=true&useSSL=true"
    // sequence as a single substring.
    assert(
      msg.contains("jdbc:mysql://h:3306/db?autoReconnect=true&useSSL=true"),
      s"URL must match canonical pattern, got: $msg"
    )
  }

  it should "use the `mysql` JDBC subprotocol (not e.g. `postgresql`)" in {
    val msg = expectFailureUrl("h", "3306", "db")
    assert(msg.contains("jdbc:mysql:"))
    assert(!msg.contains("jdbc:postgresql:"))
  }

  // ---------------------------------------------------------------------------
  // Exception type contract
  // ---------------------------------------------------------------------------

  it should "throw java.sql.SQLException (declared via @throws on the method)" in {
    val ex = intercept[SQLException] {
      MySQLConnUtil.connect("h", "3306", "db", "u", "p")
    }
    assert(ex != null)
  }
}
