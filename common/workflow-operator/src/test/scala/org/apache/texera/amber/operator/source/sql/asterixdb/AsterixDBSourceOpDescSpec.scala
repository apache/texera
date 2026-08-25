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
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.Try

class AsterixDBSourceOpDescSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // In-process AsterixDB stub
  //
  // sourceSchema() resolves the dataset's datatype over HTTP through
  // AsterixDBConnUtil, so the type-mapping tests need a reachable server. The
  // stub answers the two metadata statements sourceSchema() issues and nothing
  // else; the same approach is used by AsterixDBConnUtilSpec and
  // AsterixDBSourceOpExecSpec. Binding port 0 keeps it off any fixed port.
  // ---------------------------------------------------------------------------

  /** Field name -> AsterixDB type the stub reports for the dataset's datatype. */
  @volatile private var datatypeFields: Seq[(String, String)] = Seq.empty

  /** Decoded `statement` form field of every /query/service request, in order. */
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
    } else if (statement.contains("Metadata.`Dataset`")) {
      """{"results":[{"DatatypeName":"tweetType"}]}"""
    } else {
      // Deliberately not a fall-through onto the dataset answer: a statement
      // aimed at the wrong metadata table must come back empty, so a query the
      // descriptor mistargets cannot still yield a plausible schema.
      """{"results":[]}"""
    }

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

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    server.start()
  }

  override protected def afterAll(): Unit = {
    try {
      server.stop(0)
      // Drop only this suite's key from AsterixDBConnUtil's host-keyed version
      // cache singleton, so a sibling asterixdb suite keeps its own entry.
      AsterixDBConnUtil.asterixDBVersionMapping -= host
    } finally super.afterAll()
  }

  /** The schema sourceSchema() derives when the datatype reports these fields. */
  private def schemaOf(fields: (String, String)*): Schema = {
    datatypeFields = fields
    recordedStatements.synchronized { recordedStatements.clear() }
    val d = configured()
    d.port = port
    d.sourceSchema()
  }

  /** The statements the stub saw, in order, since the last schemaOf(). */
  private def statements: List[String] =
    recordedStatements.synchronized { recordedStatements.toList }

  /** A descriptor with every connection field filled in, ready to be broken. */
  private def configured(): AsterixDBSourceOpDesc = {
    val d = new AsterixDBSourceOpDesc
    d.host = host
    d.port = "19002"
    d.database = "test"
    d.table = "twitter"
    d
  }

  // ---------------------------------------------------------------------------
  // Operator metadata and serialization
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpDesc.operatorInfo" should
    "advertise the AsterixDB source in the Database Connector group with no input and one output" in {
    val info = (new AsterixDBSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "AsterixDB Source"
    info.operatorDescription shouldBe "Read data from an AsterixDB instance"
    info.operatorGroupName shouldBe OperatorGroupConstants.DATABASE_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "AsterixDBSourceOpDesc" should "default its geo/regex/filter and connection fields" in {
    val d = new AsterixDBSourceOpDesc
    d.geoSearch shouldBe Some(false)
    d.geoSearchByColumns shouldBe empty
    d.geoSearchBoundingBox shouldBe empty
    d.regexSearch shouldBe Some(false)
    d.regexSearchByColumn shouldBe None
    d.regex shouldBe None
    d.filterCondition shouldBe Some(false)
    d.filterPredicates shouldBe empty
    d.host shouldBe null
    d.interval shouldBe 0L
  }

  "AsterixDBSourceOpDesc.getPhysicalOp" should
    "wire the AsterixDB exec as a source op with no input port and one output port" in {
    val d = new AsterixDBSourceOpDesc
    val physical = d.getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.sql.asterixdb.AsterixDBSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "AsterixDBSourceOpDesc" should
    "round-trip its config fields and omit the ignored credentials" in {
    val d = new AsterixDBSourceOpDesc
    d.host = "localhost"
    d.database = "db"
    d.table = "t"
    d.username = "secret-user"
    d.password = "secret-pass"
    d.regex = Some("a.*")
    d.geoSearchByColumns = List("lonlat")
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"AsterixDBSource\"")
    // username/password are dropped via @JsonIgnoreProperties on this subclass.
    json should not include "secret-user"
    json should not include "secret-pass"
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[AsterixDBSourceOpDesc]
    val r = restored.asInstanceOf[AsterixDBSourceOpDesc]
    r.host shouldBe "localhost"
    r.database shouldBe "db"
    r.table shouldBe "t"
    r.regex shouldBe Some("a.*")
    r.geoSearchByColumns shouldBe List("lonlat")
    r.username shouldBe null
    r.password shouldBe null
  }

  // ---------------------------------------------------------------------------
  // updatePort
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpDesc.updatePort" should
    "resolve the sentinel `default` to AsterixDB's HTTP API port and leave any other port alone" in {
    val d = new AsterixDBSourceOpDesc
    d.port = "default"
    d.updatePort()
    d.port shouldBe "19002"

    // The sentinel is recognised through surrounding whitespace.
    d.port = "  default  "
    d.updatePort()
    d.port shouldBe "19002"

    // An explicit port is left exactly as configured.
    d.port = "19004"
    d.updatePort()
    d.port shouldBe "19004"
  }

  it should "be applied by sourceSchema before it issues any query" in {
    // `default` is the port the shipped descriptor actually carries (see
    // TestOperatorsSpec), and sourceSchema's call to updatePort is the only
    // place in production that ever resolves it. Driving updatePort directly,
    // as the test above does, cannot tell whether that call site still exists.
    val d = configured()
    d.port = "default"
    // updatePort runs after the four requires and before the first HTTP call,
    // so what the (unserved) real AsterixDB port answers is irrelevant: the
    // resolution has already happened by the time the connection is refused.
    Try(d.sourceSchema())
    d.port shouldBe "19002"
  }

  // ---------------------------------------------------------------------------
  // sourceSchema - connection validation
  //
  // Every one of these fails before any HTTP call is made, so the stub is not
  // involved. Each assertion pins the whole message, because the only job of
  // these guards is to tell the user which field is wrong.
  // ---------------------------------------------------------------------------

  "AsterixDBSourceOpDesc.sourceSchema" should
    "prompt for connection details before a connection is configured" in {
    val ex = intercept[IllegalArgumentException]((new AsterixDBSourceOpDesc).sourceSchema())
    ex.getMessage shouldBe "requirement failed: Please enter a valid host name for AsterixDB."
  }

  it should "reject a host that is only whitespace" in {
    val d = configured()
    d.host = "   "
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid host name for AsterixDB."
  }

  it should "name the port when the port is missing" in {
    val d = configured()
    d.port = null
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid port for AsterixDB."
  }

  it should "name the port when the port is only whitespace" in {
    val d = configured()
    d.port = "  "
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid port for AsterixDB."
  }

  it should "name the database when the database is missing" in {
    val d = configured()
    d.database = null
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid database name for AsterixDB."
  }

  it should "name the database when the database is only whitespace" in {
    val d = configured()
    d.database = "  "
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid database name for AsterixDB."
  }

  it should "name the table when the table is missing" in {
    val d = configured()
    d.table = null
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid table name for AsterixDB."
  }

  it should "name the table when the table is only whitespace" in {
    val d = configured()
    d.table = "  "
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid table name for AsterixDB."
  }

  it should "name the first unset field, checking host, port, database, then table" in {
    // Each test above breaks exactly one field, which pins the message attached
    // to each guard but not the order the guards run in. `require` reports only
    // the FIRST failure, so on a half-configured operator that order is the
    // whole user-visible behaviour: fixing one field must surface the next.
    val d = configured()
    d.port = "  "
    d.database = null
    d.table = ""
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid port for AsterixDB."
    d.port = "19002"
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid database name for AsterixDB."
    d.database = "test"
    intercept[IllegalArgumentException](d.sourceSchema()).getMessage shouldBe
      "requirement failed: Please enter a valid table name for AsterixDB."
  }

  // ---------------------------------------------------------------------------
  // sourceSchema - metadata resolution
  // ---------------------------------------------------------------------------

  it should "look the dataset's datatype up first, then that datatype's fields" in {
    // Two round trips, and the second has to key off the DatatypeName the first
    // one returned. Without this the whole first round trip could be discarded
    // and the schema would still come out right against a stub that answers any
    // datatype name - but against a real instance the datatype lookup would miss
    // and sourceSchema would silently return an EMPTY schema.
    schemaOf("id" -> "int64")
    statements should have length 2
    statements.head shouldBe
      "SELECT DatatypeName FROM Metadata.`Dataset` ds where ds.`DatasetName`='twitter';"
    statements(1) should include("dt.DatatypeName = 'tweetType'")
  }

  // ---------------------------------------------------------------------------
  // sourceSchema - AsterixDB datatype -> Texera attribute type
  // ---------------------------------------------------------------------------

  it should "map every AsterixDB scalar type it knows onto the matching attribute type" in {
    val schema = schemaOf(
      "a_boolean" -> "boolean",
      "b_int32" -> "int32",
      "c_int64" -> "int64",
      "d_float" -> "float",
      "e_double" -> "double",
      "f_datetime" -> "datetime",
      "g_date" -> "date",
      "h_string" -> "string"
    )
    // Attributes arrive sorted by field name.
    schema.getAttributeNames shouldBe List(
      "a_boolean",
      "b_int32",
      "c_int64",
      "d_float",
      "e_double",
      "f_datetime",
      "g_date",
      "h_string"
    )
    schema.getAttribute("a_boolean").getType shouldBe AttributeType.BOOLEAN
    schema.getAttribute("b_int32").getType shouldBe AttributeType.INTEGER
    schema.getAttribute("c_int64").getType shouldBe AttributeType.LONG
    // float and double both widen to DOUBLE.
    schema.getAttribute("d_float").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("e_double").getType shouldBe AttributeType.DOUBLE
    // date carries no time of day, but is still surfaced as a TIMESTAMP.
    schema.getAttribute("f_datetime").getType shouldBe AttributeType.TIMESTAMP
    schema.getAttribute("g_date").getType shouldBe AttributeType.TIMESTAMP
    schema.getAttribute("h_string").getType shouldBe AttributeType.STRING
  }

  it should "fall back to STRING for an AsterixDB type it does not recognise" in {
    // `uuid` is a real AsterixDB scalar type that this mapping does not name.
    // The catch-all keeps the column readable instead of failing the schema.
    schemaOf("id" -> "uuid").getAttribute("id").getType shouldBe AttributeType.STRING
  }
}
