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

package org.apache.texera.amber.operator.source.sql

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, DatabaseMetaData, ResultSet, SQLException, Types}

class SQLSourceOpDescSpec extends AnyFlatSpec with Matchers with MockFactory {

  /** Concrete subclass returning a supplied (mocked) connection. */
  private class TestSQLSourceOpDesc(conn: Connection) extends SQLSourceOpDesc {
    override def getPhysicalOp(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity
    ): PhysicalOp = ??? // never invoked by these tests
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Test SQL",
        "test",
        OperatorGroupConstants.DATABASE_GROUP,
        inputPorts = List.empty,
        outputPorts = List(OutputPort())
      )
    override protected def establishConn: Connection = conn
    override protected def updatePort(): Unit =
      port = if (port != null && port.trim.equals("default")) "1234" else port
  }

  /** Subclass that leaves establishConn as the abstract base default (null). */
  private class BaseConnSQLSourceOpDesc extends SQLSourceOpDesc {
    override def getPhysicalOp(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity
    ): PhysicalOp = ???
    override def operatorInfo: OperatorInfo =
      OperatorInfo("t", "t", OperatorGroupConstants.DATABASE_GROUP, List.empty, List(OutputPort()))
    override protected def updatePort(): Unit = {}
  }

  private def configure(desc: SQLSourceOpDesc): SQLSourceOpDesc = {
    desc.host = "h"
    desc.port = "default"
    desc.database = "db"
    desc.table = "tbl"
    desc.username = "u"
    desc.password = "p"
    desc
  }

  /** Introspect a single-column table whose one column has the given JDBC type. */
  private def attributeTypeFor(jdbcType: Int): AttributeType = {
    val rs = mock[ResultSet]
    val meta = mock[DatabaseMetaData]
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true)
    (conn.getMetaData _).expects().returning(meta)
    (meta.getColumns _).expects(null, null, "tbl", null).returning(rs)
    inSequence {
      (rs.next _).expects().returning(true)
      (rs.next _).expects().returning(false)
    }
    (rs.getString(_: String)).expects("COLUMN_NAME").returning("c")
    (rs.getInt(_: String)).expects("DATA_TYPE").returning(jdbcType)
    (conn.close _).expects()
    configure(new TestSQLSourceOpDesc(conn)).sourceSchema().getAttribute("c").getType
  }

  /** Every mandatory connection field, with the phrase its prompt must carry. */
  private val connectionFields: Seq[(String, (SQLSourceOpDesc, String) => Unit, String)] = Seq(
    ("host", (d, v) => d.host = v, "valid host name"),
    ("port", (d, v) => d.port = v, "valid port"),
    ("database", (d, v) => d.database = v, "valid database name"),
    ("table", (d, v) => d.table = v, "valid table name"),
    ("username", (d, v) => d.username = v, "valid username"),
    ("password", (d, v) => d.password = v, "valid password")
  )

  "SQLSourceOpDesc.sourceSchema" should "prompt for the missing connection field" in {
    val desc = configure(new TestSQLSourceOpDesc(mock[Connection]))
    desc.table = null
    val ex = intercept[IllegalArgumentException](desc.sourceSchema())
    ex.getMessage should include("table")
  }

  it should "prompt for each connection field left unset or blank, naming that field" in {
    // A saved workflow can carry either an absent field (null) or one the user
    // filled with whitespace; both must be rejected before any connection attempt.
    for {
      (fieldName, set, phrase) <- connectionFields
      (label, value) <- Seq("unset" -> null.asInstanceOf[String], "blank" -> "  ")
    } withClue(s"$fieldName $label: ") {
      val desc = configure(new TestSQLSourceOpDesc(mock[Connection]))
      set(desc, value)
      val ex = intercept[IllegalArgumentException](desc.sourceSchema())
      ex.getMessage should include(phrase)
    }
  }

  it should "map each JDBC data type to its Texera attribute type" in {
    // All 19 keys of the match, not a sample of them. JaCoCo folds the lookupswitch's
    // 19 cases into its 8 distinct jump targets, so this list moves coverage by
    // exactly zero -- it is bought purely so that moving a key between arms fails.
    attributeTypeFor(Types.TINYINT) shouldBe AttributeType.INTEGER
    attributeTypeFor(Types.SMALLINT) shouldBe AttributeType.INTEGER
    attributeTypeFor(Types.INTEGER) shouldBe AttributeType.INTEGER
    attributeTypeFor(Types.FLOAT) shouldBe AttributeType.DOUBLE
    attributeTypeFor(Types.REAL) shouldBe AttributeType.DOUBLE
    attributeTypeFor(Types.DOUBLE) shouldBe AttributeType.DOUBLE
    attributeTypeFor(Types.NUMERIC) shouldBe AttributeType.DOUBLE
    attributeTypeFor(Types.BIT) shouldBe AttributeType.BOOLEAN
    attributeTypeFor(Types.BOOLEAN) shouldBe AttributeType.BOOLEAN
    attributeTypeFor(Types.BINARY) shouldBe AttributeType.BINARY
    attributeTypeFor(Types.DATE) shouldBe AttributeType.STRING
    attributeTypeFor(Types.TIME) shouldBe AttributeType.STRING
    attributeTypeFor(Types.LONGVARCHAR) shouldBe AttributeType.STRING
    attributeTypeFor(Types.CHAR) shouldBe AttributeType.STRING
    attributeTypeFor(Types.VARCHAR) shouldBe AttributeType.STRING
    attributeTypeFor(Types.NULL) shouldBe AttributeType.STRING
    attributeTypeFor(Types.OTHER) shouldBe AttributeType.STRING
    attributeTypeFor(Types.BIGINT) shouldBe AttributeType.LONG
    attributeTypeFor(Types.TIMESTAMP) shouldBe AttributeType.TIMESTAMP
  }

  it should "resolve the default port and read the full column set" in {
    val rs = mock[ResultSet]
    val meta = mock[DatabaseMetaData]
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true)
    (conn.getMetaData _).expects().returning(meta)
    (meta.getColumns _).expects(null, null, "tbl", null).returning(rs)
    inSequence {
      (rs.next _).expects().returning(true)
      (rs.next _).expects().returning(true)
      (rs.next _).expects().returning(false)
    }
    inSequence {
      (rs.getString(_: String)).expects("COLUMN_NAME").returning("id")
      (rs.getString(_: String)).expects("COLUMN_NAME").returning("name")
    }
    inSequence {
      (rs.getInt(_: String)).expects("DATA_TYPE").returning(Types.INTEGER)
      (rs.getInt(_: String)).expects("DATA_TYPE").returning(Types.VARCHAR)
    }
    (conn.close _).expects()
    val desc = configure(new TestSQLSourceOpDesc(conn))
    val schema = desc.sourceSchema()
    schema.getAttributes.map(a => (a.getName, a.getType)) shouldBe List(
      ("id", AttributeType.INTEGER),
      ("name", AttributeType.STRING)
    )
    desc.port shouldBe "1234" // updatePort() resolved "default"
  }

  it should "reject an unknown JDBC data type" in {
    val rs = mock[ResultSet]
    val meta = mock[DatabaseMetaData]
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true)
    (conn.getMetaData _).expects().returning(meta)
    (meta.getColumns _).expects(null, null, "tbl", null).returning(rs)
    (rs.next _).expects().returning(true)
    (rs.getString(_: String)).expects("COLUMN_NAME").returning("c")
    (rs.getInt(_: String)).expects("DATA_TYPE").returning(Types.ARRAY)
    val ex = intercept[RuntimeException](configure(new TestSQLSourceOpDesc(conn)).sourceSchema())
    // Whole message, not endWith: the leading getSimpleName is the only thing that
    // tells the user WHICH source operator in the workflow rejected the column.
    ex.getMessage shouldBe s"TestSQLSourceOpDesc: unknown data type: ${Types.ARRAY}"
  }

  it should "wrap a SQLException raised while introspecting" in {
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true).throwing(new SQLException("boom"))
    val ex = intercept[RuntimeException](configure(new TestSQLSourceOpDesc(conn)).sourceSchema())
    ex.getMessage shouldBe "TestSQLSourceOpDesc failed to connect to the database. boom"
  }

  it should "wrap a ClassCastException raised while introspecting" in {
    // A driver handing back a mistyped metadata object surfaces as a CCE, which
    // the descriptor declares as a connection failure rather than letting it escape.
    val conn = mock[Connection]
    (conn.setReadOnly _).expects(true).throwing(new ClassCastException("cce"))
    val ex = intercept[RuntimeException](configure(new TestSQLSourceOpDesc(conn)).sourceSchema())
    ex.getClass shouldBe classOf[RuntimeException]
    ex.getMessage shouldBe "TestSQLSourceOpDesc failed to connect to the database. cce"
  }

  it should "fail when the base establishConn returns no connection" in {
    intercept[NullPointerException](configure(new BaseConnSQLSourceOpDesc).sourceSchema())
  }
}
