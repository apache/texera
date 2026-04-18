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
import org.scalatest.flatspec.AnyFlatSpec

class MySQLSourceOpDescSpec extends AnyFlatSpec {

  behavior of "MySQLSourceOpDesc.mapMySQLType"

  it should "map integer-family types to INTEGER" in {
    for (t <- Seq("tinyint", "smallint", "mediumint", "int", "integer"))
      assert(MySQLSourceOpDesc.mapMySQLType(t) == AttributeType.INTEGER)
  }

  it should "map bigint to LONG" in {
    assert(MySQLSourceOpDesc.mapMySQLType("bigint") == AttributeType.LONG)
  }

  it should "map float-family types to DOUBLE" in {
    for (t <- Seq("float", "double", "real", "decimal", "numeric"))
      assert(MySQLSourceOpDesc.mapMySQLType(t) == AttributeType.DOUBLE)
  }

  it should "map bit/boolean/bool to BOOLEAN" in {
    for (t <- Seq("bit", "boolean", "bool"))
      assert(MySQLSourceOpDesc.mapMySQLType(t) == AttributeType.BOOLEAN)
  }

  it should "map binary-family types to BINARY" in {
    for (t <- Seq("binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob"))
      assert(MySQLSourceOpDesc.mapMySQLType(t) == AttributeType.BINARY)
  }

  it should "map text-like and temporal-date-only types to STRING" in {
    for (t <- Seq(
           "date",
           "time",
           "year",
           "char",
           "varchar",
           "tinytext",
           "text",
           "mediumtext",
           "longtext",
           "enum",
           "set",
           "json"
         ))
      assert(MySQLSourceOpDesc.mapMySQLType(t) == AttributeType.STRING)
  }

  it should "map timestamp and datetime to TIMESTAMP" in {
    assert(MySQLSourceOpDesc.mapMySQLType("timestamp") == AttributeType.TIMESTAMP)
    assert(MySQLSourceOpDesc.mapMySQLType("datetime") == AttributeType.TIMESTAMP)
  }

  it should "throw on unknown types" in {
    assertThrows[RuntimeException](MySQLSourceOpDesc.mapMySQLType("uuid"))
  }

  behavior of "MySQLSourceOpDesc.updatePort"

  it should "resolve 'default' to 3306" in {
    val desc = new MySQLSourceOpDesc()
    desc.port = "default"
    desc.updatePort()
    assert(desc.port == "3306")
  }

  it should "preserve a user-supplied port" in {
    val desc = new MySQLSourceOpDesc()
    desc.port = "3307"
    desc.updatePort()
    assert(desc.port == "3307")
  }

  behavior of "MySQLSourceOpDesc.sourceSchema"

  it should "return null when any required connection field is missing" in {
    val desc = new MySQLSourceOpDesc()
    // host, database, table, username, password all left null — must short-circuit
    assert(desc.sourceSchema() == null)
  }
}
