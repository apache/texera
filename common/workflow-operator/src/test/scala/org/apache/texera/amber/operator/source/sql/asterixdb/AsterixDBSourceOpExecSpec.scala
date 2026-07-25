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

package org.apache.texera.amber.operator.source.sql.asterixdb

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Tuple}
import org.apache.texera.amber.operator.filter.{ComparisonType, FilterPredicate}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import scala.collection.mutable

/**
  * Tests for AsterixDBSourceOpExec. The executor has no JDBC layer: every round
  * trip goes over HTTP through AsterixDBConnUtil, and even the constructor needs
  * a reachable server because `schema = desc.sourceSchema()` fetches the dataset's
  * datatype metadata. So the suite stands an in-process HTTP stub in for an
  * AsterixDB instance (same approach as AsterixDBConnUtilSpec) and drives the
  * executor against it. That keeps the SQL++ string building — the bulk of this
  * class — under test end to end, together with the CSV row -> Tuple conversion.
  *
  * The stub binds port 0 so no fixed port is required, and the suite uses the
  * host spelling `localhost` (AsterixDBConnUtilSpec uses `127.0.0.1`) so the two
  * suites never collide in AsterixDBConnUtil's host-keyed version cache singleton.
  */
class AsterixDBSourceOpExecSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  // ---------------------------------------------------------------------------
  // In-process AsterixDB stub
  // ---------------------------------------------------------------------------

  // Metadata the stub reports for the `twitter` dataset. Attributes reach the
  // schema sorted by name, so the schema is:
  //   count INTEGER, created_at TIMESTAMP, id LONG, score DOUBLE,
  //   text STRING, verified BOOLEAN
  private val datatypeFields = Seq(
    "id" -> "int64",
    "text" -> "string",
    "count" -> "int32",
    "score" -> "double",
    "created_at" -> "datetime",
    "verified" -> "boolean"
  )

  // Response to the `select DatasetName from Metadata.Dataset` health check that
  // SQLSourceOpExec.open() uses to validate the configured table name.
  @volatile private var tableNameRows: Seq[String] = Seq("\"twitter\"\n")
  // Response to every non-metadata statement, i.e. the generated data queries.
  @volatile private var dataRows: Seq[String] = Seq.empty
  // Decoded `statement` form field of every /query/service request, in order.
  private val recordedStatements = mutable.Buffer[String]()

  private val server: HttpServer = HttpServer.create(new InetSocketAddress(0), 0)
  server.createContext(
    "/admin/version",
    (exchange: HttpExchange) => respond(exchange, """{"git.build.version":"0.9.9"}""")
  )
  server.createContext(
    "/query/service",
    (exchange: HttpExchange) => {
      val is = exchange.getRequestBody
      val body =
        try new String(is.readAllBytes(), StandardCharsets.UTF_8)
        finally is.close()
      val statement = formField(body, "statement")
      recordedStatements.synchronized { recordedStatements += statement }
      respond(exchange, responseFor(statement))
    }
  )

  private val host = "localhost"
  private def port: String = server.getAddress.getPort.toString

  private def responseFor(statement: String): String =
    if (statement.contains("Metadata.`Datatype`")) {
      val fields = datatypeFields
        .map { case (name, tpe) => s"""{"FieldName":"$name","FieldType":"$tpe"}""" }
        .mkString(",")
      s"""{"results":[{"Fields":[$fields]}]}"""
    } else if (statement.contains("SELECT DatatypeName FROM Metadata.`Dataset`")) {
      """{"results":[{"DatatypeName":"tweetType"}]}"""
    } else if (statement.contains("select `DatasetName` from Metadata.`Dataset`")) {
      resultsOf(tableNameRows)
    } else {
      resultsOf(dataRows)
    }

  private def resultsOf(rows: Seq[String]): String =
    s"""{"results":[${rows.map(jsonString).mkString(",")}]}"""

  private def jsonString(raw: String): String =
    "\"" + raw
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n") + "\""

  private def respond(exchange: HttpExchange, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(200, bytes.length.toLong)
    val os = exchange.getResponseBody
    try os.write(bytes)
    finally os.close()
  }

  private def formField(body: String, name: String): String =
    body
      .split("&")
      .filter(_.contains("="))
      .map { pair =>
        val idx = pair.indexOf('=')
        URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8) ->
          URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8)
      }
      .toMap
      .getOrElse(name, "")

  /** Statements the executor issued that are not metadata bookkeeping. */
  private def dataStatements: Seq[String] =
    recordedStatements.synchronized {
      recordedStatements.toList.filterNot(s =>
        s.contains("Metadata.`Dataset`") || s.contains("Metadata.`Datatype`")
      )
    }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    server.start()
  }

  override protected def afterAll(): Unit = {
    try {
      server.stop(0)
      // Drop only this suite's key so a concurrently running AsterixDBConnUtilSpec
      // keeps its own cache entry.
      AsterixDBConnUtil.asterixDBVersionMapping -= host
    } finally super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    tableNameRows = Seq("\"twitter\"\n")
    dataRows = Seq.empty
    recordedStatements.synchronized { recordedStatements.clear() }
  }

  // ---------------------------------------------------------------------------
  // Executor fixtures
  // ---------------------------------------------------------------------------

  private def newExec(configure: AsterixDBSourceOpDesc => Unit = _ => ()): AsterixDBSourceOpExec = {
    val desc = new AsterixDBSourceOpDesc
    desc.host = host
    desc.port = port
    desc.database = "test"
    desc.table = "twitter"
    configure(desc)
    new AsterixDBSourceOpExec(objectMapper.writeValueAsString(desc))
  }

  private def filterQuery(configure: AsterixDBSourceOpDesc => Unit): String = {
    val queryBuilder = new StringBuilder
    newExec(configure).addFilterConditions(queryBuilder)
    queryBuilder.result()
  }

  private def drain(exec: AsterixDBSourceOpExec): List[Tuple] =
    exec.produceTuple().map(_.asInstanceOf[Tuple]).toList

  private val allSearchesOn: AsterixDBSourceOpDesc => Unit = { desc =>
    desc.keywordSearch = Some(true)
    desc.keywordSearchByColumn = Some("text")
    desc.keywords = Some("['a'], {'mode':'any'}")
    desc.regexSearch = Some(true)
    desc.regexSearchByColumn = Some("text")
    desc.regex = Some("a.*")
    desc.geoSearch = Some(true)
    desc.geoSearchByColumns = List("place")
    desc.geoSearchBoundingBox = List("1,2", "3,4")
    desc.filterCondition = Some(true)
    desc.filterPredicates = List(new FilterPredicate("id", ComparisonType.GREATER_THAN, "5"))
  }

  // ---------------------------------------------------------------------------
  // Schema derived from the dataset metadata
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpExec" should "derive its schema from the dataset's AsterixDB datatype" in {
    val exec = newExec()
    exec.schema.getAttributeNames shouldBe
      List("count", "created_at", "id", "score", "text", "verified")
    exec.schema.getAttribute("count").getType shouldBe AttributeType.INTEGER
    exec.schema.getAttribute("created_at").getType shouldBe AttributeType.TIMESTAMP
    exec.schema.getAttribute("id").getType shouldBe AttributeType.LONG
    exec.schema.getAttribute("score").getType shouldBe AttributeType.DOUBLE
    exec.schema.getAttribute("text").getType shouldBe AttributeType.STRING
    exec.schema.getAttribute("verified").getType shouldBe AttributeType.BOOLEAN
  }

  // ---------------------------------------------------------------------------
  // addFilterConditions - keyword search
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpExec.addFilterConditions" should
    "leave the query untouched when no search or filter toggle is enabled" in {
    filterQuery(_ => ()) shouldBe ""
  }

  it should "append the incoming builder rather than replacing it" in {
    val queryBuilder = new StringBuilder("SELECT * FROM t WHERE 1 = 1")
    newExec { desc =>
      desc.keywordSearch = Some(true)
      desc.keywordSearchByColumn = Some("text")
      desc.keywords = Some("['a'], {'mode':'all'}")
    }.addFilterConditions(queryBuilder)
    queryBuilder.result() shouldBe
      "SELECT * FROM t WHERE 1 = 1 AND ftcontains(text, ['a'], {'mode':'all'}) "
  }

  it should "emit an ftcontains predicate for a keyword search on a string column" in {
    filterQuery { desc =>
      desc.keywordSearch = Some(true)
      desc.keywordSearchByColumn = Some("text")
      desc.keywords = Some("['hello', 'world'], {'mode':'any'}")
    } shouldBe " AND ftcontains(text, ['hello', 'world'], {'mode':'any'}) "
  }

  it should "reject a keyword search on a non-string column" in {
    val ex = intercept[IllegalArgumentException] {
      filterQuery { desc =>
        desc.keywordSearch = Some(true)
        desc.keywordSearchByColumn = Some("id")
        desc.keywords = Some("['hello'], {'mode':'any'}")
      }
    }
    ex.getMessage should startWith("Can't do keyword search on type")
  }

  it should "skip the keyword search when the column or the keywords are missing" in {
    filterQuery { desc =>
      desc.keywordSearch = Some(true)
      desc.keywordSearchByColumn = Some("text")
    } shouldBe ""
    filterQuery { desc =>
      desc.keywordSearch = Some(true)
      desc.keywords = Some("['hello'], {'mode':'any'}")
    } shouldBe ""
  }

  it should "skip the keyword search when the toggle is off even if it is fully configured" in {
    filterQuery { desc =>
      desc.keywordSearch = Some(false)
      desc.keywordSearchByColumn = Some("id")
      desc.keywords = Some("['hello'], {'mode':'any'}")
    } shouldBe ""
  }

  // ---------------------------------------------------------------------------
  // addFilterConditions - regex search
  // ---------------------------------------------------------------------------

  it should "emit a regexp_contains predicate with the regex quoted, for a string column" in {
    filterQuery { desc =>
      desc.regexSearch = Some(true)
      desc.regexSearchByColumn = Some("text")
      desc.regex = Some("^a.*z$")
    } shouldBe " AND regexp_contains(text, \"^a.*z$\") "
  }

  it should "reject a regex search on a non-string column" in {
    val ex = intercept[IllegalArgumentException] {
      filterQuery { desc =>
        desc.regexSearch = Some(true)
        desc.regexSearchByColumn = Some("score")
        desc.regex = Some("a.*")
      }
    }
    ex.getMessage should startWith("Can't do regex search on type")
  }

  it should "skip the regex search when the column or the pattern is missing" in {
    filterQuery { desc =>
      desc.regexSearch = Some(true)
      desc.regexSearchByColumn = Some("text")
    } shouldBe ""
    filterQuery { desc =>
      desc.regexSearch = Some(true)
      desc.regex = Some("a.*")
    } shouldBe ""
  }

  it should "skip the regex search when the toggle is off even if it is fully configured" in {
    filterQuery { desc =>
      desc.regexSearch = Some(false)
      desc.regexSearchByColumn = Some("id")
      desc.regex = Some("a.*")
    } shouldBe ""
  }

  // ---------------------------------------------------------------------------
  // addFilterConditions - geo search
  // ---------------------------------------------------------------------------

  it should "build a create_rectangle shape from exactly two bounding box corners" in {
    filterQuery { desc =>
      desc.geoSearch = Some(true)
      desc.geoSearchByColumns = List("place.bbox")
      desc.geoSearchBoundingBox = List("-118.4,33.9", "-117.5,34.5")
    } shouldBe " AND (spatial_intersect(place.bbox, create_rectangle(" +
      "create_point(-118.400000,33.900000), create_point(-117.500000,34.500000))) ) "
  }

  it should "build a create_polygon shape from more than two bounding box points" in {
    filterQuery { desc =>
      desc.geoSearch = Some(true)
      desc.geoSearchByColumns = List("place.bbox")
      desc.geoSearchBoundingBox = List("1,2", "3,4", "5.5,6")
    } shouldBe " AND (spatial_intersect(place.bbox, create_polygon(" +
      "[1.000000,2.000000,3.000000,4.000000,5.500000,6.000000])) ) "
  }

  it should "OR one spatial_intersect per geo column against the same shape" in {
    val result = filterQuery { desc =>
      desc.geoSearch = Some(true)
      desc.geoSearchByColumns = List("place.bbox", "coordinate")
      desc.geoSearchBoundingBox = List("1,2", "3,4")
    }
    val shape = "create_rectangle(create_point(1.000000,2.000000), create_point(3.000000,4.000000))"
    result shouldBe s" AND (spatial_intersect(place.bbox, $shape) OR " +
      s"spatial_intersect(coordinate, $shape) ) "
  }

  it should "skip the geo search when the bounding box has fewer than two points" in {
    filterQuery { desc =>
      desc.geoSearch = Some(true)
      desc.geoSearchByColumns = List("place.bbox")
      desc.geoSearchBoundingBox = List("1,2")
    } shouldBe ""
  }

  it should "skip the geo search when no geo column is selected" in {
    filterQuery { desc =>
      desc.geoSearch = Some(true)
      desc.geoSearchByColumns = List.empty
      desc.geoSearchBoundingBox = List("1,2", "3,4")
    } shouldBe ""
  }

  it should "skip the geo search when the toggle is off even if it is fully configured" in {
    filterQuery { desc =>
      desc.geoSearch = Some(false)
      desc.geoSearchByColumns = List("place.bbox")
      desc.geoSearchBoundingBox = List("1,2", "3,4")
    } shouldBe ""
  }

  it should "fail loudly on a bounding box point that is not numeric" in {
    intercept[NumberFormatException] {
      filterQuery { desc =>
        desc.geoSearch = Some(true)
        desc.geoSearchByColumns = List("place.bbox")
        desc.geoSearchBoundingBox = List("1,2", "3,north")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // addFilterConditions - general filter predicates
  // ---------------------------------------------------------------------------

  it should "OR the general filter predicates inside a single parenthesised group" in {
    filterQuery { desc =>
      desc.filterCondition = Some(true)
      desc.filterPredicates = List(
        new FilterPredicate("id", ComparisonType.GREATER_THAN, "5"),
        new FilterPredicate("text", ComparisonType.EQUAL_TO, "'x'"),
        new FilterPredicate("score", ComparisonType.LESS_THAN_OR_EQUAL_TO, "1.5")
      )
    } shouldBe " AND ( (id > 5) OR (text = 'x') OR (score <= 1.5) ) "
  }

  it should "skip the general filter when the predicate list is empty" in {
    filterQuery { desc =>
      desc.filterCondition = Some(true)
      desc.filterPredicates = List.empty
    } shouldBe ""
  }

  it should "skip the general filter when the toggle is off even if predicates exist" in {
    filterQuery { desc =>
      desc.filterCondition = Some(false)
      desc.filterPredicates = List(new FilterPredicate("id", ComparisonType.GREATER_THAN, "5"))
    } shouldBe ""
  }

  // ---------------------------------------------------------------------------
  // addFilterConditions - composition
  // ---------------------------------------------------------------------------

  it should "concatenate keyword, regex, geo and general filters in that order" in {
    val result = filterQuery(allSearchesOn)
    val positions = List(
      result.indexOf("ftcontains("),
      result.indexOf("regexp_contains("),
      result.indexOf("spatial_intersect("),
      result.indexOf("(id > 5)")
    )
    positions should not contain -1
    positions shouldBe positions.sorted
    // Every clause is joined with AND, so the whole group narrows the result set.
    result.sliding(" AND ".length).count(_ == " AND ") shouldBe 4
  }

  // ---------------------------------------------------------------------------
  // addBaseSelect / addLimit / addOffset
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpExec.addBaseSelect" should
    "project every schema attribute through if_missing under a positional alias" in {
    val queryBuilder = new StringBuilder
    newExec().addBaseSelect(queryBuilder)
    queryBuilder.result() should startWith(
      "\nSELECT if_missing(count,null) field_0, if_missing(created_at,null) field_1, " +
        "if_missing(id,null) field_2, if_missing(score,null) field_3, " +
        "if_missing(text,null) field_4, if_missing(verified,null) field_5 FROM "
    )
    queryBuilder.result() should endWith(" WHERE 1 = 1 ")
  }

  it should "qualify the FROM clause with the configured database and table" in {
    val exec = newExec()
    val queryBuilder = new StringBuilder
    exec.addBaseSelect(queryBuilder)
    val query = queryBuilder.result()
    query should include(" FROM test.twitter WHERE 1 = 1 ")
    // The descriptor itself must never leak into the query: `s"$desc.database"`
    // renders LogicalOp.toString (a reflective dump) followed by literal
    // ".database", which is what this used to emit.
    query should not include exec.desc.toString
  }

  "AsterixDBSourceOpExec.addLimit" should "inline the remaining limit rather than bind a parameter" in {
    val exec = newExec()
    exec.curLimit = Some(50L)
    val queryBuilder = new StringBuilder
    exec.addLimit(queryBuilder)
    queryBuilder.result() shouldBe " LIMIT 50"
  }

  "AsterixDBSourceOpExec.addOffset" should "inline the remaining offset rather than bind a parameter" in {
    val exec = newExec()
    exec.curOffset = Some(7L)
    val queryBuilder = new StringBuilder
    exec.addOffset(queryBuilder)
    queryBuilder.result() shouldBe " OFFSET 7"
  }

  // ---------------------------------------------------------------------------
  // batchAttributeToString
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpExec.batchAttributeToString" should
    "render numeric batch columns as plain literals" in {
    val exec = newExec()
    exec.batchByAttribute = Some(new Attribute("id", AttributeType.LONG))
    exec.batchAttributeToString(java.lang.Long.valueOf(42L)) shouldBe "42"
    exec.batchByAttribute = Some(new Attribute("count", AttributeType.INTEGER))
    exec.batchAttributeToString(java.lang.Integer.valueOf(42)) shouldBe "42"
    exec.batchByAttribute = Some(new Attribute("score", AttributeType.DOUBLE))
    exec.batchAttributeToString(java.lang.Double.valueOf(1.5d)) shouldBe "1.5"
  }

  it should "render a timestamp batch column as a UTC datetime() constructor" in {
    val exec = newExec()
    exec.batchByAttribute = Some(new Attribute("created_at", AttributeType.TIMESTAMP))
    // 1699870530000 == 2023-11-13T10:15:30Z; the formatter pins UTC, so the
    // rendering must not drift with the JVM default time zone.
    exec.batchAttributeToString(java.lang.Long.valueOf(1699870530000L)) shouldBe
      "datetime('2023-11-13T10:15:30')"
  }

  it should "reject batch columns whose type cannot be incremented" in {
    val exec = newExec()
    exec.batchByAttribute = Some(new Attribute("text", AttributeType.STRING))
    intercept[IllegalArgumentException] {
      exec.batchAttributeToString(java.lang.Long.valueOf(1L))
    }.getMessage should startWith("Unexpected type:")
    exec.batchByAttribute = Some(new Attribute("verified", AttributeType.BOOLEAN))
    intercept[IllegalArgumentException] {
      exec.batchAttributeToString(java.lang.Long.valueOf(1L))
    }.getMessage should startWith("Unexpected type:")
  }

  it should "reject the call when no batch column is resolved" in {
    val exec = newExec(_.batchByColumn = Some("created_at"))
    exec.batchByAttribute shouldBe None
    intercept[IllegalArgumentException] {
      exec.batchAttributeToString(java.lang.Long.valueOf(1L))
    }.getMessage shouldBe "No valid batchByColumn to iterate: created_at"
  }

  // ---------------------------------------------------------------------------
  // fetchBatchByBoundary
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpExec.fetchBatchByBoundary" should
    "query the aggregate over database.table and return the numeric boundary" in {
    val exec = newExec()
    exec.batchByAttribute = Some(new Attribute("id", AttributeType.LONG))
    dataRows = Seq("100\n")
    exec.fetchBatchByBoundary("MAX") shouldBe 100L
    dataStatements should contain("SELECT MAX(id) FROM test.twitter;")
    dataRows = Seq("3\n")
    exec.fetchBatchByBoundary("MIN") shouldBe 3L
    dataStatements should contain("SELECT MIN(id) FROM test.twitter;")
  }

  it should "convert a timestamp boundary to epoch milliseconds" in {
    val exec = newExec()
    exec.batchByAttribute = Some(new Attribute("created_at", AttributeType.TIMESTAMP))
    dataRows = Seq("\"2023-11-13T10:15:30\"\n")
    exec.fetchBatchByBoundary("MIN") shouldBe
      Timestamp.valueOf("2023-11-13 10:15:30").getTime
  }

  it should "fall back to 0 when the boundary value cannot be parsed as the column type" in {
    val exec = newExec()
    exec.batchByAttribute = Some(new Attribute("id", AttributeType.LONG))
    dataRows = Seq("not-a-number\n")
    exec.fetchBatchByBoundary("MAX") shouldBe 0
  }

  it should "return 0 without issuing any query when there is no batch column" in {
    val exec = newExec()
    exec.batchByAttribute shouldBe None
    exec.fetchBatchByBoundary("MAX") shouldBe 0
    dataStatements shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // open / loadTableNames
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpExec.open" should
    "load the dataset names, stripping their CSV quoting, and accept the configured table" in {
    val exec = newExec()
    tableNameRows = Seq("\"other\"\n", "\"twitter\"\n")
    exec.open()
    exec.tableNames.toList shouldBe List("other", "twitter")
    AsterixDBConnUtil.asterixDBVersionMapping.get(host) shouldBe Some("0.9.9")
  }

  it should "reject a table that the AsterixDB instance does not expose" in {
    val exec = newExec()
    tableNameRows = Seq("\"other\"\n")
    val ex = intercept[RuntimeException](exec.open())
    ex.getMessage shouldBe "Can't find the given table `twitter`."
  }

  // ---------------------------------------------------------------------------
  // produceTuple / buildTupleFromRow
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpExec.produceTuple" should
    "issue the generated SQL++ query once and convert every CSV row to a typed Tuple" in {
    val exec = newExec()
    dataRows = Seq(
      "7,2023-11-13T10:15:30,100,1.5,\"hello, world\",true",
      "8,2023-11-14T10:15:30,101,-2.25,plain,false"
    )
    val tuples = drain(exec)

    tuples should have length 2
    tuples.head.getField[Any]("count") shouldBe 7
    tuples.head.getField[Timestamp]("created_at").toString shouldBe "2023-11-13 10:15:30.0"
    tuples.head.getField[Any]("id") shouldBe 100L
    tuples.head.getField[Any]("score") shouldBe 1.5d
    // The quoted CSV field keeps its embedded delimiter and loses its quotes.
    tuples.head.getField[Any]("text") shouldBe "hello, world"
    tuples.head.getField[Any]("verified") shouldBe true
    tuples(1).getField[Any]("count") shouldBe 8
    tuples(1).getField[Any]("score") shouldBe -2.25d
    tuples(1).getField[Any]("text") shouldBe "plain"
    tuples(1).getField[Any]("verified") shouldBe false

    // One data query for the whole scan, terminated and carrying the projection.
    dataStatements should have length 1
    dataStatements.head should include("SELECT if_missing(count,null) field_0")
    dataStatements.head should endWith(";")
  }

  it should "map the literal token null, in any column, to a null field" in {
    val exec = newExec()
    dataRows = Seq("null,null,null,null,null,null")
    val tuple = drain(exec).head
    exec.schema.getAttributeNames.foreach(name => tuple.getField[AnyRef](name) shouldBe null)
  }

  it should "null out the trailing columns a short CSV row does not supply" in {
    val exec = newExec()
    dataRows = Seq("7,2023-11-13T10:15:30,100")
    val tuple = drain(exec).head
    tuple.getField[Any]("count") shouldBe 7
    tuple.getField[Any]("id") shouldBe 100L
    tuple.getField[AnyRef]("score") shouldBe null
    tuple.getField[AnyRef]("text") shouldBe null
    tuple.getField[AnyRef]("verified") shouldBe null
  }

  it should "ignore CSV columns beyond the schema width" in {
    val exec = newExec()
    dataRows = Seq("7,2023-11-13T10:15:30,100,1.5,text,true,extra,more")
    val tuple = drain(exec).head
    tuple.getFields should have length 6
    tuple.getField[Any]("verified") shouldBe true
  }

  it should "drop a row whose value does not parse as the declared column type" in {
    val exec = newExec()
    dataRows = Seq(
      "not-an-int,2023-11-13T10:15:30,100,1.5,bad,true",
      "8,2023-11-14T10:15:30,101,2.5,good,false"
    )
    val tuples = drain(exec)
    tuples should have length 1
    tuples.head.getField[Any]("text") shouldBe "good"
  }

  it should "produce nothing when the query comes back with no rows" in {
    val exec = newExec()
    dataRows = Seq.empty
    drain(exec) shouldBe empty
    dataStatements should have length 1
  }

  it should "carry the filter conditions into the generated query" in {
    val exec = newExec { desc =>
      desc.keywordSearch = Some(true)
      desc.keywordSearchByColumn = Some("text")
      desc.keywords = Some("['hello'], {'mode':'any'}")
      desc.filterCondition = Some(true)
      desc.filterPredicates = List(new FilterPredicate("id", ComparisonType.GREATER_THAN, "5"))
    }
    drain(exec) shouldBe empty
    dataStatements.head should include(" AND ftcontains(text, ['hello'], {'mode':'any'}) ")
    dataStatements.head should include(" AND ( (id > 5) ) ")
  }

  it should "inline LIMIT and OFFSET into the query and count them down per row" in {
    val exec = newExec()
    exec.curLimit = Some(2L)
    exec.curOffset = Some(1L)
    // AsterixDB applies LIMIT/OFFSET server side, so the stub answers the
    // generated `LIMIT 2 OFFSET 1` with the corresponding window of the three
    // rows it holds: "first" is skipped by the OFFSET, "second" and "third"
    // come back.
    dataRows = Seq(
      "2,2023-11-13T10:15:30,2,2.0,second,true",
      "3,2023-11-13T10:15:30,3,3.0,third,true"
    )
    val tuples = drain(exec)
    dataStatements.head should include(" LIMIT 2")
    dataStatements.head should include(" OFFSET 1")
    tuples.map(_.getField[Any]("text")) shouldBe List("second", "third")
    // The client-side offset skip is a no-op for this executor: the loop only
    // peeks with `hasNext` before breaking - unlike the JDBC base class, whose
    // `resultSet.next()` actually advances past the skipped row - so no
    // server-filtered row is dropped a second time. Both counters still wind
    // down to zero, which is what stops the next query from being generated.
    exec.curOffset shouldBe Some(0L)
    exec.curLimit shouldBe Some(0L)
  }

  it should "stop the query generation entirely once the remaining limit reaches zero" in {
    val exec = newExec()
    exec.curLimit = Some(0L)
    dataRows = Seq("1,2023-11-13T10:15:30,1,1.0,first,true")
    drain(exec) shouldBe empty
    dataStatements shouldBe empty
  }

  "AsterixDBSourceOpExec.close" should "discard the in-flight result iterator and query" in {
    val exec = newExec()
    dataRows = Seq(
      "1,2023-11-13T10:15:30,1,1.0,first,true",
      "2,2023-11-13T10:15:30,2,2.0,second,true"
    )
    val iterator = exec.produceTuple()
    iterator.next().asInstanceOf[Tuple].getField[Any]("text") shouldBe "first"
    exec.close()
    // The partially consumed iterator is gone and no further query is issued,
    // so the second row is never delivered.
    exec.produceTuple().hasNext shouldBe false
    dataStatements should have length 1
  }

  it should "leave a partially consumed scan resumable when it is not closed" in {
    val exec = newExec()
    dataRows = Seq(
      "1,2023-11-13T10:15:30,1,1.0,first,true",
      "2,2023-11-13T10:15:30,2,2.0,second,true"
    )
    val iterator = exec.produceTuple()
    iterator.next().asInstanceOf[Tuple].getField[Any]("text") shouldBe "first"
    exec.produceTuple().next().asInstanceOf[Tuple].getField[Any]("text") shouldBe "second"
    dataStatements should have length 1
  }
}
