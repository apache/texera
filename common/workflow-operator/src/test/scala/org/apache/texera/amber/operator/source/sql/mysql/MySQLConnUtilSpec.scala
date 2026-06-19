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

class MySQLConnUtilSpec extends AnyFlatSpec {

  // Strategy: same as PostgreSQLConnUtilSpec. Pin the JDBC URL composition
  // (the only application-logic in this util) without a real DB.

  // ---------------------------------------------------------------------------
  // URL composition - host/port/database
  // ---------------------------------------------------------------------------

  "MySQLConnUtil.buildJdbcUrl" should
    "build a URL of the form jdbc:mysql://{host}:{port}/{database}?..." in {
    assert(
      MySQLConnUtil
        .buildJdbcUrl("host-m", "3306", "db-m")
        .startsWith("jdbc:mysql://host-m:3306/db-m")
    )
  }

  it should "interpolate distinct host/port/database values into the URL" in {
    assert(
      MySQLConnUtil
        .buildJdbcUrl("host-1", "3306", "db-1")
        .startsWith("jdbc:mysql://host-1:3306/db-1")
    )
    assert(
      MySQLConnUtil
        .buildJdbcUrl("host-2", "33060", "db-2")
        .startsWith("jdbc:mysql://host-2:33060/db-2")
    )
  }

  it should "place host BEFORE port" in {
    val url = MySQLConnUtil.buildJdbcUrl("a", "1", "x")
    assert(url.contains("//a:1/"))
    assert(!url.contains("//1:a/"))
  }

  // ---------------------------------------------------------------------------
  // Query parameters - autoReconnect=true and useSSL=true must be present
  // ---------------------------------------------------------------------------

  it should "include the `autoReconnect=true` query parameter" in {
    val url = MySQLConnUtil.buildJdbcUrl("h", "3306", "db")
    assert(url.contains("autoReconnect=true"), s"URL must include autoReconnect=true, got: $url")
  }

  it should "include the `useSSL=true` query parameter (TLS contract)" in {
    val url = MySQLConnUtil.buildJdbcUrl("h", "3306", "db")
    assert(url.contains("useSSL=true"), s"URL must include useSSL=true (TLS), got: $url")
  }

  it should "use the canonical `?...&...` separator pattern" in {
    assert(
      MySQLConnUtil.buildJdbcUrl(
        "h",
        "3306",
        "db"
      ) == "jdbc:mysql://h:3306/db?autoReconnect=true&useSSL=true"
    )
  }

  it should "use the `mysql` JDBC subprotocol (not e.g. `postgresql`)" in {
    val url = MySQLConnUtil.buildJdbcUrl("h", "3306", "db")
    assert(url.startsWith("jdbc:mysql://"))
    assert(!url.contains("jdbc:postgresql:"))
  }
}
