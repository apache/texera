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

import org.scalatest.flatspec.AnyFlatSpec

class PostgreSQLConnUtilSpec extends AnyFlatSpec {

  // Strategy: pin the JDBC URL composition (the only application-logic in
  // this util) without a real DB.

  // ---------------------------------------------------------------------------
  // URL composition - pin the exact JDBC URL
  // ---------------------------------------------------------------------------

  "PostgreSQLConnUtil.buildJdbcUrl" should
    "build a URL of the form jdbc:postgresql://{host}:{port}/{database}" in {
    assert(
      PostgreSQLConnUtil.buildJdbcUrl(
        "host-a",
        "5432",
        "db-a"
      ) == "jdbc:postgresql://host-a:5432/db-a"
    )
  }

  it should "interpolate distinct host/port/database values into the URL" in {
    assert(
      PostgreSQLConnUtil.buildJdbcUrl("h-1", "1234", "d-1") == "jdbc:postgresql://h-1:1234/d-1"
    )
    assert(
      PostgreSQLConnUtil.buildJdbcUrl("h-2", "9999", "d-2") == "jdbc:postgresql://h-2:9999/d-2"
    )
  }

  it should "place host BEFORE port (host-then-port, not port-then-host)" in {
    val url = PostgreSQLConnUtil.buildJdbcUrl("a", "1", "x")
    assert(url.contains("//a:1/"), s"expected //a:1/ ordering, got: $url")
    assert(!url.contains("//1:a/"), s"port-then-host ordering must NOT appear, got: $url")
  }

  it should "use the `postgresql` JDBC subprotocol (not e.g. `mysql`)" in {
    val url = PostgreSQLConnUtil.buildJdbcUrl("h", "5432", "db")
    assert(url.startsWith("jdbc:postgresql://"))
    assert(!url.contains("jdbc:mysql:"))
  }

  it should "accept an empty database name and still produce a well-formed URL" in {
    // The resulting `jdbc:postgresql://h:5432/` is well-formed even if a
    // real driver would reject it.
    assert(PostgreSQLConnUtil.buildJdbcUrl("h", "5432", "") == "jdbc:postgresql://h:5432/")
  }
}
