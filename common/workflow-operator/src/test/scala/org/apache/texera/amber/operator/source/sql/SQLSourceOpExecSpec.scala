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

import org.apache.texera.amber.core.tuple.AttributeTypeUtils.parseTimestamp
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.operator.source.sql.postgresql.PostgreSQLSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}

class SQLSourceOpExecSpec extends AnyFlatSpec with Matchers with MockFactory {

  private val BASE = "\nSELECT * FROM tbl where 1 = 1"

  private val fullSchema = Schema(
    List(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING),
      new Attribute("score", AttributeType.DOUBLE),
      new Attribute("big", AttributeType.LONG),
      new Attribute("ts", AttributeType.TIMESTAMP)
    )
  )

  private val rowSchema = Schema(
    List(new Attribute("id", AttributeType.INTEGER), new Attribute("name", AttributeType.STRING))
  )

  /** `rowSchema` plus the LONG column the progressive row tests batch by. */
  private val progressiveRowSchema = Schema(
    List(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING),
      new Attribute("big", AttributeType.LONG)
    )
  )

  /**
    * Minimal concrete executor: the DB-touching hooks are stubbed out so the
    * query-building surface can be exercised on its own.
    */
  private class TestSQLSourceOpExec(
      descString: String,
      conn: Connection = null,
      filter: String = "",
      knownTables: Seq[String] = Seq("tbl"),
      execSchema: Schema = fullSchema
  ) extends SQLSourceOpExec(descString) {
    schema = execSchema
    var loadTableNamesCalls = 0

    override protected def establishConn(): Connection = conn

    override protected def loadTableNames(): Unit = {
      loadTableNamesCalls += 1
      tableNames ++= knownTables
    }

    override protected def addFilterConditions(queryBuilder: StringBuilder): Unit =
      queryBuilder ++= filter

    // expose the protected query-building surface
    private def render(f: StringBuilder => Unit): String = {
      val builder = new StringBuilder
      f(builder)
      builder.result()
    }

    def sqlQuery: Option[String] = generateSqlQuery
    def nextQueryAvailable: Boolean = hasNextQuery
    def baseSelect: String = render(b => addBaseSelect(b))
    def limitClause: String = render(b => addLimit(b))
    def offsetClause: String = render(b => addOffset(b))
    def terminator: String = render(b => terminateSQL(b))
    def slidingWindow: String = render(b => addBatchSlidingWindow(b))
    def batchValue(value: Number): String = batchAttributeToString(value)
    def boundary(side: String): Number = fetchBatchByBoundary(side)
  }

  private def descJson(configure: PostgreSQLSourceOpDesc => Unit = _ => ()): String = {
    val desc = new PostgreSQLSourceOpDesc
    desc.host = "localhost"
    desc.port = "5432"
    desc.database = "db"
    desc.table = "tbl"
    desc.username = "u"
    desc.password = "p"
    configure(desc)
    objectMapper.writeValueAsString(desc)
  }

  private def progressiveJson(
      column: String,
      min: Option[String],
      max: Option[String],
      interval: Long
  ): String =
    descJson { desc =>
      desc.progressive = Option(true)
      desc.batchByColumn = Option(column)
      desc.min = min
      desc.max = max
      desc.interval = interval
    }

  /** Builds an opened executor in progressive mode. */
  private def openedProgressive(
      column: String,
      min: String,
      max: String,
      interval: Long,
      conn: Connection = null
  ): TestSQLSourceOpExec = {
    val exec =
      new TestSQLSourceOpExec(progressiveJson(column, Option(min), Option(max), interval), conn)
    exec.open()
    exec
  }

  /**
    * A connection answering the `SELECT MIN/MAX(col) FROM tbl;` boundary probes.
    * `sides` narrows it to the probes the executor is expected to actually issue.
    */
  private def boundaryConn(
      column: String,
      stub: (ResultSet, String) => Unit,
      sides: Seq[String] = Seq("MIN", "MAX")
  ): Connection = {
    val conn = mock[Connection]
    sides.foreach { side =>
      val statement = mock[PreparedStatement]
      val resultSet = mock[ResultSet]
      (conn
        .prepareStatement(_: String))
        .expects(s"SELECT $side($column) FROM tbl;")
        .returning(statement)
      (statement.executeQuery: () => ResultSet).expects().returning(resultSet)
      (resultSet.next _).expects().returning(true)
      stub(resultSet, side)
      (resultSet.close _).expects()
      (statement.close _).expects()
    }
    conn
  }

  "SQLSourceOpExec" should "deserialize the descriptor and emit the individual SQL fragments" in {
    val exec = new TestSQLSourceOpExec(descJson())
    exec.desc.table shouldBe "tbl"
    exec.desc.database shouldBe "db"
    exec.desc.progressive shouldBe Option(false)
    exec.baseSelect shouldBe BASE
    exec.limitClause shouldBe " LIMIT ?"
    exec.offsetClause shouldBe " OFFSET ?"
    exec.terminator shouldBe ";"
  }

  it should "generate the plain query when no limit, offset or filter applies" in {
    new TestSQLSourceOpExec(descJson()).sqlQuery shouldBe Some(BASE + ";")
  }

  it should "splice in the subclass filter conditions" in {
    val exec = new TestSQLSourceOpExec(descJson(), filter = " AND name @@ to_tsquery(?)")
    exec.sqlQuery shouldBe Some(BASE + " AND name @@ to_tsquery(?);")
  }

  it should "append LIMIT and OFFSET according to the current cursor state" in {
    val exec = new TestSQLSourceOpExec(descJson())

    exec.curLimit = Some(5L)
    exec.sqlQuery shouldBe Some(BASE + " LIMIT ?;")

    exec.curOffset = Some(7L)
    exec.sqlQuery shouldBe Some(BASE + " LIMIT ? OFFSET ?;")

    exec.curLimit = None
    exec.sqlQuery shouldBe Some(BASE + " OFFSET ?;")
  }

  it should "produce no query once the remaining limit is exhausted" in {
    val exec = new TestSQLSourceOpExec(descJson())
    exec.curOffset = Some(3L)

    exec.curLimit = Some(0L)
    exec.sqlQuery shouldBe None

    exec.curLimit = Some(-4L)
    exec.sqlQuery shouldBe None
  }

  it should "omit the fixed OFFSET in progressive mode and use a sliding window instead" in {
    val exec = openedProgressive("big", "0", "100", 30L)
    exec.curOffset = Some(9L)
    exec.sqlQuery shouldBe Some(BASE + " AND big >= 0 AND big < 30;")
  }

  it should "skip the sliding window when the batch interval is not positive" in {
    val exec = openedProgressive("big", "0", "100", 0L)
    exec.sqlQuery shouldBe Some(BASE + ";")
  }

  it should "skip the sliding window when progressive mode names no batch column" in {
    // A progressive descriptor with no batchByColumn is degenerate but generateSqlQuery is
    // defensive about it. open() is deliberately not called here: it would blow up on
    // `desc.batchByColumn.get`, which is the current (unhelpful) behaviour, so this test
    // pins only the query builder. That also means this exact arm of the line-419 guard
    // (progressive with no batch column) is defensive: no production caller reaches
    // generateSqlQuery in this state, since open() throws first.
    // min and max are populated on purpose: with all three of batchByColumn/min/max empty,
    // re-pointing the guard at `desc.min` instead would be indistinguishable here.
    val exec = new TestSQLSourceOpExec(descJson { desc =>
      desc.progressive = Option(true)
      desc.batchByColumn = None
      desc.min = Option("0")
      desc.max = Option("100")
      desc.interval = 30L
    })
    exec.sqlQuery shouldBe Some(BASE + ";")
  }

  it should "ignore a leftover batch column when progressive mode is off" in {
    // `progressive` only toggles batchByColumn/min/max/interval *hidden* in the UI
    // (SQLSourceOpDesc's toggleHidden), it does not clear them, so a non-progressive
    // descriptor can still carry a batch column. Neither the sliding window nor the
    // fixed OFFSET may key off that column instead of the progressive flag.
    val exec = new TestSQLSourceOpExec(descJson { desc =>
      desc.batchByColumn = Option("big")
      desc.min = Option("0")
      desc.max = Option("100")
      desc.interval = 30L
    })
    exec.curOffset = Some(5L)
    exec.sqlQuery shouldBe Some(BASE + " OFFSET ?;")
  }

  it should "walk the LONG batch column window by window until the upper bound" in {
    // Deliberately NOT pinned: the `>=` that decides the last window (SQLSourceOpExec's
    // `isLastBatch`, and its DOUBLE twin) is only observable when the upper bound sits
    // exactly one interval above the current lower bound -- and on that input production
    // emits a redundant `>= upper AND <= upper` window that re-scans the boundary row.
    // Asserting the current behaviour there would cement that duplication, so it is
    // reported as a defect rather than encoded here.
    val exec = openedProgressive("big", "0", "100", 30L)

    exec.nextQueryAvailable shouldBe true
    exec.slidingWindow shouldBe " AND big >= 0 AND big < 30"
    exec.slidingWindow shouldBe " AND big >= 30 AND big < 60"
    exec.slidingWindow shouldBe " AND big >= 60 AND big < 90"
    exec.nextQueryAvailable shouldBe true
    // the final window is inclusive of the upper bound
    exec.slidingWindow shouldBe " AND big >= 90 AND big <= 100"
    exec.nextQueryAvailable shouldBe false
  }

  it should "render TIMESTAMP batch boundaries as quoted timestamps" in {
    val start = parseTimestamp("2023-01-01 00:00:00").getTime
    val exec =
      openedProgressive("ts", "2023-01-01 00:00:00", "2023-01-01 00:00:10", 4000L)

    exec.slidingWindow shouldBe
      s" AND ts >= '${new Timestamp(start)}' AND ts < '${new Timestamp(start + 4000)}'"
    exec.slidingWindow shouldBe
      s" AND ts >= '${new Timestamp(start + 4000)}' AND ts < '${new Timestamp(start + 8000)}'"
    exec.slidingWindow shouldBe
      s" AND ts >= '${new Timestamp(start + 8000)}' AND ts <= '${new Timestamp(start + 10000)}'"
    exec.nextQueryAvailable shouldBe false
  }

  it should "stringify LONG batch values without quotes" in {
    val exec = openedProgressive("big", "0", "100", 30L)
    exec.batchValue(Long.box(42L)) shouldBe "42"
  }

  it should "reject non-auto boundaries on a column that is neither LONG nor TIMESTAMP" in {
    val exec = new TestSQLSourceOpExec(progressiveJson("score", Option("1"), Option("9"), 4L))
    val thrown = intercept[IllegalArgumentException](exec.open())
    thrown.getMessage shouldBe "Unsupported type double"
    // batchByAttribute is already resolved, so the pure formatting path stays reachable
    exec.batchValue(Double.box(1.5d)) shouldBe "1.5"
  }

  it should "stringify INTEGER batch values without quotes" in {
    val exec = new TestSQLSourceOpExec(progressiveJson("id", Option("1"), Option("9"), 4L))
    intercept[IllegalArgumentException](exec.open())
    exec.batchValue(Int.box(7)) shouldBe "7"
  }

  it should "refuse to format a batch value of an unsupported attribute type" in {
    val exec = new TestSQLSourceOpExec(progressiveJson("name", Option("a"), Option("z"), 4L))
    intercept[IllegalArgumentException](exec.open())
    val thrown = intercept[IllegalArgumentException](exec.batchValue(Int.box(1)))
    thrown.getMessage shouldBe "Unexpected type: string"
  }

  it should "refuse to build a sliding window for an unsupported attribute type" in {
    val exec = new TestSQLSourceOpExec(progressiveJson("name", Option("a"), Option("z"), 4L))
    intercept[IllegalArgumentException](exec.open())
    val thrown = intercept[IllegalArgumentException](exec.slidingWindow)
    thrown.getMessage shouldBe "Unexpected type: string"
  }

  it should "refuse to build a sliding window when no batch column is configured" in {
    val exec = new TestSQLSourceOpExec(descJson())
    // the two call sites word the message identically but differ in the capitalization of "no";
    // pin the semantics, not the casing, so normalizing it later stays a harmless change
    intercept[IllegalArgumentException](exec.slidingWindow).getMessage should
      fullyMatch regex "(?i)no valid batchByColumn to iterate: "
    intercept[IllegalArgumentException](exec.batchValue(Int.box(1))).getMessage should
      fullyMatch regex "(?i)no valid batchByColumn to iterate: "
  }

  it should "offer exactly one query when progressive mode is disabled" in {
    val exec = new TestSQLSourceOpExec(descJson())
    exec.nextQueryAvailable shouldBe true
    exec.nextQueryAvailable shouldBe false
    exec.nextQueryAvailable shouldBe false
  }

  it should "offer a next query while an INTEGER batch column is below its upper bound" in {
    val conn = boundaryConn(
      "id",
      (rs, side) => (rs.getInt(_: Int)).expects(1).returning(if (side == "MIN") 0 else 10)
    )
    val exec = openedProgressive("id", "auto", "auto", 4L, conn)

    exec.nextQueryAvailable shouldBe true
    exec.slidingWindow shouldBe " AND id >= 0 AND id < 4"

    // A single-value range (min == max) must still yield exactly one window. This is the
    // one input on which the `<=` in hasNextQuery is load-bearing, and unlike the
    // exact-multiple case it does not exercise the duplicate-last-window defect.
    val singleValueConn =
      boundaryConn("id", (rs, _) => (rs.getInt(_: Int)).expects(1).returning(7))
    val singleValue = openedProgressive("id", "auto", "auto", 4L, singleValueConn)
    singleValue.nextQueryAvailable shouldBe true
    singleValue.slidingWindow shouldBe " AND id >= 7 AND id <= 7"
    singleValue.nextQueryAvailable shouldBe false
  }

  it should "refuse to decide the next query for an unsupported batch column type" in {
    val exec = new TestSQLSourceOpExec(progressiveJson("name", Option("a"), Option("z"), 4L))
    // DEFENSIVE-ONLY arm: intercepting open() is the only way to be holding a STRING batch
    // column here, so no workflow can reach this throw. A successful open() admits only
    // INTEGER/LONG/TIMESTAMP/DOUBLE -- non-auto boundaries reject all but LONG/TIMESTAMP,
    // and auto probes reject BOOLEAN/STRING/ANY -- and all four are handled above.
    intercept[IllegalArgumentException](exec.open())
    intercept[IllegalArgumentException](exec.nextQueryAvailable).getMessage shouldBe
      "Unexpected type: string"
  }

  "SQLSourceOpExec.open" should "validate the table against the loaded table names" in {
    val exec = new TestSQLSourceOpExec(descJson(), knownTables = Seq("other"))
    val thrown = intercept[RuntimeException](exec.open())
    thrown.getMessage shouldBe "Can't find the given table `tbl`."
    exec.loadTableNamesCalls shouldBe 1
  }

  it should "validate the table before probing the batch column boundaries" in {
    // fetchBatchByBoundary concatenates desc.table straight into the probe SQL, so the
    // table-name check -- which this file documents as its SQL-injection defence -- has to
    // run before any boundary probe. The strict mock with no expectations is the oracle:
    // any probe at all is an unexpected call.
    val conn = mock[Connection]
    val exec = new TestSQLSourceOpExec(
      progressiveJson("big", Option("auto"), Option("auto"), 30L),
      conn,
      knownTables = Seq("other")
    )
    intercept[RuntimeException](exec.open()).getMessage shouldBe "Can't find the given table `tbl`."
  }

  it should "leave batchByAttribute unset when progressive mode is disabled" in {
    val exec = new TestSQLSourceOpExec(descJson())
    exec.open()
    exec.batchByAttribute shouldBe None
    exec.tableNames should contain("tbl")
  }

  it should "reject progressive mode without min and max boundaries" in {
    val exec = new TestSQLSourceOpExec(progressiveJson("big", None, None, 30L))
    val thrown = intercept[IllegalArgumentException](exec.open())
    thrown.getMessage should startWith("Missing required progressive configuration")
  }

  it should "reject progressive mode with a lower but no upper boundary" in {
    val exec = new TestSQLSourceOpExec(progressiveJson("big", Option("0"), None, 30L))
    val thrown = intercept[IllegalArgumentException](exec.open())
    thrown.getMessage should startWith("Missing required progressive configuration")
  }

  it should "reject progressive mode with an upper but no lower boundary" in {
    // the mirror of the case above: without this one, dropping the min check from the
    // configuration guard would go unnoticed and desc.min.get would raise a bare
    // NoSuchElementException instead of this message
    val exec = new TestSQLSourceOpExec(progressiveJson("big", None, Option("100"), 30L))
    val thrown = intercept[IllegalArgumentException](exec.open())
    thrown.getMessage should startWith("Missing required progressive configuration")
  }

  it should "reject a non-auto upper bound on a column that is neither LONG nor TIMESTAMP" in {
    // the MIN side resolves through the auto probe, so the MAX side is the one that
    // reaches the type match and rejects the DOUBLE column
    val conn = boundaryConn(
      "score",
      (rs, _) => (rs.getDouble(_: Int)).expects(1).returning(1.5),
      sides = Seq("MIN")
    )
    val exec =
      new TestSQLSourceOpExec(progressiveJson("score", Option("auto"), Option("5"), 4L), conn)
    intercept[IllegalArgumentException](exec.open()).getMessage shouldBe "Unsupported type double"
  }

  it should "fetch auto boundaries for every supported batch column type" in {
    val intConn = boundaryConn(
      "id",
      (rs, side) => (rs.getInt(_: Int)).expects(1).returning(if (side == "MIN") 0 else 10)
    )
    openedProgressive("id", "auto", "auto", 4L, intConn).slidingWindow shouldBe
      " AND id >= 0 AND id < 4"

    val longConn = boundaryConn(
      "big",
      (rs, side) => (rs.getLong(_: Int)).expects(1).returning(if (side == "MIN") 5L else 25L)
    )
    openedProgressive("big", "AUTO", "Auto", 10L, longConn).slidingWindow shouldBe
      " AND big >= 5 AND big < 15"

    val start = parseTimestamp("2024-03-01 10:00:00").getTime
    val tsConn = boundaryConn(
      "ts",
      (rs, side) =>
        (rs
          .getTimestamp(_: Int))
          .expects(1)
          .returning(new Timestamp(if (side == "MIN") start else start + 10000))
    )
    openedProgressive("ts", "auto", "auto", 4000L, tsConn).slidingWindow shouldBe
      s" AND ts >= '${new Timestamp(start)}' AND ts < '${new Timestamp(start + 4000)}'"
  }

  it should "walk a DOUBLE batch column resolved from auto boundaries" in {
    val conn = boundaryConn(
      "score",
      (rs, side) => (rs.getDouble(_: Int)).expects(1).returning(if (side == "MIN") 1.5 else 9.0)
    )
    val exec = openedProgressive("score", "auto", "auto", 4L, conn)

    exec.nextQueryAvailable shouldBe true
    exec.slidingWindow shouldBe " AND score >= 1.5 AND score < 5.5"
    exec.slidingWindow shouldBe " AND score >= 5.5 AND score <= 9.0"
    exec.nextQueryAvailable shouldBe false

    // the DOUBLE mirror of the INTEGER single-value case: min == max must still yield one
    // window, which is the only input that makes this arm's `<=` load-bearing
    val singleValueConn =
      boundaryConn("score", (rs, _) => (rs.getDouble(_: Int)).expects(1).returning(1.5))
    val singleValue = openedProgressive("score", "auto", "auto", 4L, singleValueConn)
    singleValue.nextQueryAvailable shouldBe true
    singleValue.slidingWindow shouldBe " AND score >= 1.5 AND score <= 1.5"
    singleValue.nextQueryAvailable shouldBe false
  }

  it should "reject an auto boundary probe on an unsupported column type" in {
    val conn = mock[Connection]
    val statement = mock[PreparedStatement]
    val resultSet = mock[ResultSet]
    (conn
      .prepareStatement(_: String))
      .expects("SELECT MIN(name) FROM tbl;")
      .returning(statement)
    (statement.executeQuery: () => ResultSet).expects().returning(resultSet)
    (resultSet.next _).expects().returning(true)
    // the probe currently throws before closing; allow (but don't require) the close-out so a
    // later fix that releases the ResultSet/PreparedStatement on this path won't break the spec
    (resultSet.close _).expects().anyNumberOfTimes()
    (statement.close _).expects().anyNumberOfTimes()

    val exec =
      new TestSQLSourceOpExec(progressiveJson("name", Option("auto"), Option("auto"), 4L), conn)
    intercept[IllegalStateException](exec.open()).getMessage shouldBe "Unexpected value: string"
  }

  it should "report a zero boundary when no batch column is configured" in {
    // DEFENSIVE-ONLY arm, not a reachable execution path: fetchBatchByBoundary is called from
    // exactly two production sites, both inside initBatchColumnBoundaries behind
    // `batchByAttribute.isDefined`, and AsterixDBSourceOpExec overrides the method outright.
    // What this pins is the protected contract subclasses inherit, nothing a workflow can run.
    // no connection is needed: without a batchByAttribute the probe never touches JDBC
    new TestSQLSourceOpExec(descJson()).boundary("MIN").intValue shouldBe 0
  }

  "SQLSourceOpExec.produceTuple" should "stream every row of the single non-progressive query" in {
    val resultSet = mock[ResultSet]
    val statement = mock[PreparedStatement]
    val conn = mock[Connection]

    (conn.prepareStatement(_: String)).expects(BASE + ";").returning(statement)
    (statement.executeQuery: () => ResultSet).expects().returning(resultSet)
    inSequence {
      (resultSet.next _).expects().returning(true)
      (resultSet.next _).expects().returning(true)
      (resultSet.next _).expects().returning(false)
    }
    inSequence {
      (resultSet.getObject(_: String)).expects("id").returning(Int.box(1))
      (resultSet.getObject(_: String)).expects("id").returning(Int.box(2))
    }
    inSequence {
      (resultSet.getObject(_: String)).expects("name").returning("a")
      (resultSet.getObject(_: String)).expects("name").returning(null)
    }
    (resultSet.close _).expects()
    (statement.close _).expects()

    val exec = new TestSQLSourceOpExec(descJson(), conn, execSchema = rowSchema)
    exec.open()
    val tuples = exec.produceTuple().map(_.asInstanceOf[Tuple]).toList

    tuples should have size 2
    tuples.map(_.getField[Any]("id")) shouldBe List(1, 2)
    tuples.map(_.getField[Any]("name")) shouldBe List("a", null)
  }

  it should "bind the keyword, limit and offset parameters and skip the offset rows" in {
    val resultSet = mock[ResultSet]
    val statement = mock[PreparedStatement]
    val conn = mock[Connection]

    (conn
      .prepareStatement(_: String))
      .expects(BASE + " AND name @@ to_tsquery(?) LIMIT ? OFFSET ?;")
      .returning(statement)
    (statement.setString _).expects(1, "foo")
    (statement.setLong _).expects(2, 2L)
    (statement.setLong _).expects(3, 1L)
    (statement.executeQuery: () => ResultSet).expects().returning(resultSet)
    inSequence {
      (resultSet.next _).expects().returning(true) // consumed by the manual offset skip
      (resultSet.next _).expects().returning(true)
      (resultSet.next _).expects().returning(false)
    }
    (resultSet.getObject(_: String)).expects("id").returning(Int.box(9))
    (resultSet.getObject(_: String)).expects("name").returning("z")
    (resultSet.close _).expects()
    (statement.close _).expects()

    val exec = new TestSQLSourceOpExec(
      descJson { desc =>
        desc.keywordSearch = Option(true)
        desc.keywordSearchByColumn = Option("name")
        desc.keywords = Option("foo")
      },
      conn,
      filter = " AND name @@ to_tsquery(?)",
      execSchema = rowSchema
    )
    exec.curLimit = Some(2L)
    exec.curOffset = Some(1L)
    exec.open()

    val tuples = exec.produceTuple().map(_.asInstanceOf[Tuple]).toList
    tuples.map(_.getField[Any]("id")) shouldBe List(9)
    exec.curOffset shouldBe Some(0L)
    exec.curLimit shouldBe Some(1L)
  }

  it should "bind no fixed offset in progressive mode" in {
    val resultSet = mock[ResultSet]
    val statement = mock[PreparedStatement]
    val conn = mock[Connection]

    // the limit is present so that progressive-ness and "has a limit" are distinguishable
    // here, and so that the clause order (sliding window ahead of LIMIT) is pinned
    (conn
      .prepareStatement(_: String))
      .expects(BASE + " AND big >= 0 AND big < 30 LIMIT ?;")
      .returning(statement)
    // only the limit is bound, at index 1: progressive mode carries its offset in the
    // sliding window, and the strict mock rejects any further setLong, i.e. any offset
    (statement.setLong _).expects(1, 2L)
    (statement.executeQuery: () => ResultSet).expects().returning(resultSet)
    inSequence {
      (resultSet.next _).expects().returning(true) // consumed by the manual offset skip
      (resultSet.next _).expects().returning(true)
    }
    (resultSet.getObject(_: String)).expects("id").returning(Int.box(4))
    (resultSet.getObject(_: String)).expects("name").returning("p")
    (resultSet.getObject(_: String)).expects("big").returning(Long.box(7L))

    val exec = new TestSQLSourceOpExec(
      progressiveJson("big", Option("0"), Option("100"), 30L),
      conn,
      execSchema = progressiveRowSchema
    )
    exec.curOffset = Some(1L)
    exec.curLimit = Some(2L)
    exec.open()

    val first = exec.produceTuple().next().asInstanceOf[Tuple]
    first.getField[Any]("id") shouldBe 4
    first.getField[Any]("big") shouldBe 7L
    exec.curOffset shouldBe Some(0L)
    exec.curLimit shouldBe Some(1L)
  }

  it should "yield nothing when the remaining limit already forbids a query" in {
    val conn = mock[Connection]
    val exec = new TestSQLSourceOpExec(descJson(), conn, execSchema = rowSchema)
    exec.curLimit = Some(0L)
    exec.open()
    exec.produceTuple().hasNext shouldBe false
  }

  "SQLSourceOpExec.close" should "close the established connection" in {
    val conn = mock[Connection]
    (conn.close _).expects()
    val exec = new TestSQLSourceOpExec(descJson(), conn)
    exec.open()
    exec.close()
  }

  it should "tolerate being closed without a connection" in {
    noException should be thrownBy new TestSQLSourceOpExec(descJson()).close()
  }

  /*
   * The result iterator and the keyword binding. Everything above builds SQL strings; these
   * drive the executor's Iterator against a mocked JDBC chain, which is what the untaken
   * arms on hasNext/next and the three-way keyword guard need.
   */

  /** A connection answering one query with the given (id, name) rows, then exhausting. */
  private def rowsConn(rows: Seq[(Any, Any)]): Connection = {
    val conn = mock[Connection]
    val statement = mock[PreparedStatement]
    val resultSet = mock[ResultSet]
    (conn.prepareStatement(_: String)).expects(*).returning(statement)
    (statement.executeQuery: () => ResultSet).expects().returning(resultSet)
    inSequence {
      rows.foreach {
        case (id, name) =>
          (resultSet.next _).expects().returning(true)
          (resultSet.getObject(_: String)).expects("id").returning(id)
          (resultSet.getObject(_: String)).expects("name").returning(name)
      }
      (resultSet.next _).expects().returning(false)
    }
    (resultSet.close _).expects()
    (statement.close _).expects()
    conn
  }

  private def openedPlain(
      conn: Connection,
      descriptor: String = descJson()
  ): TestSQLSourceOpExec = {
    val exec = new TestSQLSourceOpExec(descriptor, conn, execSchema = rowSchema)
    exec.open()
    exec
  }

  it should "yield one tuple per row and then report exhaustion" in {
    val exec = openedPlain(rowsConn(Seq((1, "a"), (2, "b"))))
    val it = exec.produceTuple()

    it.hasNext shouldBe true
    // A second hasNext must not consume the cached tuple.
    it.hasNext shouldBe true
    it.next().asInstanceOf[Tuple].getField[Integer]("id") shouldBe 1
    it.hasNext shouldBe true
    it.next().asInstanceOf[Tuple].getField[Integer]("id") shouldBe 2

    // The result set is drained and no further query is available.
    it.hasNext shouldBe false
  }

  it should "report exhaustion immediately for a query that returns no rows" in {
    val exec = openedPlain(rowsConn(Seq.empty))

    exec.produceTuple().hasNext shouldBe false
  }

  it should "bind the keyword only when the search is enabled and both column and keywords are set" in {
    def bindsKeyword(
        enabled: Boolean,
        column: Option[String],
        keywords: Option[String],
        expectBinding: Boolean
    ): Unit = {
      val conn = mock[Connection]
      val statement = mock[PreparedStatement]
      val resultSet = mock[ResultSet]
      (conn.prepareStatement(_: String)).expects(*).returning(statement)
      if (expectBinding) (statement.setString _).expects(1, keywords.get)
      else (statement.setString _).expects(*, *).never()
      (statement.executeQuery: () => ResultSet).expects().returning(resultSet)
      (resultSet.next _).expects().returning(false)
      (resultSet.close _).expects()
      (statement.close _).expects()

      val descriptor = descJson { desc =>
        desc.keywordSearch = Option(enabled)
        desc.keywordSearchByColumn = column
        desc.keywords = keywords
      }
      openedPlain(conn, descriptor).produceTuple().hasNext shouldBe false
    }

    // each conjunct decides the outcome once
    bindsKeyword(enabled = false, Option("name"), Option("term"), expectBinding = false)
    bindsKeyword(enabled = true, None, Option("term"), expectBinding = false)
    bindsKeyword(enabled = true, Option("name"), None, expectBinding = false)
    bindsKeyword(enabled = true, Option("name"), Option("term"), expectBinding = true)
  }

}
