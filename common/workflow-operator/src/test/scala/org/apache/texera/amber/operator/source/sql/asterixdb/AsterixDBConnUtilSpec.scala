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
import kong.unirest.json.JSONException
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/**
  * Characterization tests for AsterixDBConnUtil. Unlike the JDBC ConnUtils, this
  * util talks plain HTTP via Unirest, so it is exercised end-to-end against an
  * in-process HTTP stub server (same approach as LiteLLMProxyAuthSpec) standing
  * in for an AsterixDB instance. No network dependency; the server binds port 0
  * to pick any free ephemeral port.
  */
class AsterixDBConnUtilSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  // ---------------------------------------------------------------------------
  // In-process AsterixDB stub
  // ---------------------------------------------------------------------------

  private val versionHits = new AtomicInteger(0)
  @volatile private var versionStatus: Int = 200
  @volatile private var versionBody: String = versionJson("0.9.9")
  // Responds to POST /query/service based on the submitted statement.
  @volatile private var queryResponder: String => (Int, String) =
    _ => (200, """{"results":[]}""")
  // Decoded form fields of every /query/service request, in arrival order.
  private val recordedQueries = mutable.Buffer[Map[String, String]]()

  private val server: HttpServer = HttpServer.create(new InetSocketAddress(0), 0)
  server.createContext(
    "/admin/version",
    (exchange: HttpExchange) => {
      versionHits.incrementAndGet()
      respond(exchange, versionStatus, versionBody)
    }
  )
  server.createContext(
    "/query/service",
    (exchange: HttpExchange) => {
      val is = exchange.getRequestBody
      val body =
        try new String(is.readAllBytes(), StandardCharsets.UTF_8)
        finally is.close()
      val form = parseForm(body)
      recordedQueries.synchronized { recordedQueries += form }
      val (status, responseBody) = queryResponder(form.getOrElse("statement", ""))
      respond(exchange, status, responseBody)
    }
  )

  private val host = "127.0.0.1"
  private def port: String = server.getAddress.getPort.toString

  private def versionJson(version: String): String =
    s"""{"git.build.version":"$version"}"""

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    if (status == 204) {
      // 204 must not carry a body; length -1 signals a bodyless response.
      exchange.sendResponseHeaders(204, -1)
      exchange.close()
    } else {
      val bytes = body.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.add("Content-Type", "application/json")
      exchange.sendResponseHeaders(status, bytes.length.toLong)
      val os = exchange.getResponseBody
      try os.write(bytes)
      finally os.close()
    }
  }

  private def parseForm(body: String): Map[String, String] =
    body
      .split("&")
      .filter(_.contains("="))
      .map { pair =>
        val idx = pair.indexOf('=')
        URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8) ->
          URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8)
      }
      .toMap

  private def lastQueryField(name: String): String =
    recordedQueries.synchronized { recordedQueries.last(name) }

  override protected def beforeAll(): Unit = server.start()

  override protected def afterAll(): Unit = {
    server.stop(0)
    // Don't leak this suite's entries into the shared-JVM singleton.
    AsterixDBConnUtil.asterixDBVersionMapping.clear()
  }

  override protected def beforeEach(): Unit = {
    // The version cache is a mutable singleton keyed by host; reset it so every
    // test starts from a cold cache, along with the stub's canned responses.
    AsterixDBConnUtil.asterixDBVersionMapping.clear()
    versionHits.set(0)
    versionStatus = 200
    versionBody = versionJson("0.9.9")
    queryResponder = _ => (200, """{"results":[]}""")
    recordedQueries.synchronized { recordedQueries.clear() }
  }

  // ---------------------------------------------------------------------------
  // queryAsterixDB - version cache population and reuse
  // ---------------------------------------------------------------------------

  "AsterixDBConnUtil.queryAsterixDB" should
    "resolve and cache the server version on first use, hitting /admin/version only once" in {
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 2;")
    versionHits.get shouldBe 1
    AsterixDBConnUtil.asterixDBVersionMapping.get(host) shouldBe Some("0.9.9")
  }

  it should "skip /admin/version entirely when the version cache is already populated" in {
    AsterixDBConnUtil.asterixDBVersionMapping += (host -> "0.9.9")
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    versionHits.get shouldBe 0
  }

  it should "key the version cache by host, leaving other hosts' entries untouched" in {
    AsterixDBConnUtil.asterixDBVersionMapping += ("some-other-host" -> "0.9.5")
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    versionHits.get shouldBe 1
    AsterixDBConnUtil.asterixDBVersionMapping.get(host) shouldBe Some("0.9.9")
    AsterixDBConnUtil.asterixDBVersionMapping.get("some-other-host") shouldBe Some("0.9.5")
  }

  // ---------------------------------------------------------------------------
  // queryAsterixDB - version-dependent `format` request field
  // ---------------------------------------------------------------------------

  it should "send the default format `csv` unprefixed when the server version is 0.9.5" in {
    versionBody = versionJson("0.9.5")
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    lastQueryField("format") shouldBe "csv"
  }

  it should "send format `text/csv` when the server version is not 0.9.5" in {
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    lastQueryField("format") shouldBe "text/csv"
    // The statement is forwarded verbatim as the `statement` form field.
    lastQueryField("statement") shouldBe "SELECT 1;"
  }

  it should "apply the text/ prefix to a caller-supplied format on non-0.9.5 servers" in {
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;", format = "JSON")
    lastQueryField("format") shouldBe "text/JSON"
  }

  it should "pass a caller-supplied format through unprefixed on 0.9.5 servers" in {
    AsterixDBConnUtil.asterixDBVersionMapping += (host -> "0.9.5")
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;", format = "JSON")
    lastQueryField("format") shouldBe "JSON"
  }

  it should "prefix the format unless the version is exactly 0.9.5 (equals, not startsWith)" in {
    AsterixDBConnUtil.asterixDBVersionMapping += (host -> "0.9.5-SNAPSHOT")
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    lastQueryField("format") shouldBe "text/csv"
    AsterixDBConnUtil.asterixDBVersionMapping += (host -> "0.9.50")
    AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    lastQueryField("format") shouldBe "text/csv"
  }

  it should "deliver a statement with quotes, form metacharacters, and non-ASCII text intact" in {
    // The statement travels as an x-www-form-urlencoded field; characters that
    // collide with the form encoding (& = % +), quotes, newlines, and non-ASCII
    // text must all survive the encode/decode round trip byte-identically.
    val statement =
      "SELECT v FROM t WHERE a = 'x&y=z' AND b = '100% + 1' AND c = '名字'\nORDER BY v;"
    AsterixDBConnUtil.queryAsterixDB(host, port, statement)
    lastQueryField("statement") shouldBe statement
  }

  // ---------------------------------------------------------------------------
  // queryAsterixDB - result and error paths
  // ---------------------------------------------------------------------------

  it should "return the `results` array as an iterator on HTTP 200" in {
    queryResponder = _ => (200, """{"results":["a","b",3]}""")
    val result = AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT x FROM y;")
    result shouldBe defined
    result.get.toList.map(_.toString) shouldBe List("a", "b", "3")
  }

  it should "return an empty iterator when `results` is an empty array" in {
    val result = AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT x FROM y;")
    result shouldBe defined
    result.get.hasNext shouldBe false
  }

  it should "throw a RuntimeException embedding status and body on a non-200 response" in {
    queryResponder = _ => (500, """{"errors":[{"msg":"syntax error near boom"}]}""")
    val ex = intercept[RuntimeException] {
      AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT bad;")
    }
    ex.getMessage should include("Send query to asterix failed")
    ex.getMessage should include("error status:")
    ex.getMessage should include("error body:")
    ex.getMessage should include("syntax error near boom")
  }

  it should "fail with NoSuchElementException when the version endpoint is unavailable" in {
    // Pins current behavior: a failed version probe leaves the cache unset and
    // the subsequent unguarded cache lookup blows up instead of degrading.
    versionStatus = 503
    intercept[NoSuchElementException] {
      AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    }
    versionHits.get shouldBe 1
    AsterixDBConnUtil.asterixDBVersionMapping.contains(host) shouldBe false
    // The failure happens before any POST to /query/service is sent.
    recordedQueries.synchronized { recordedQueries shouldBe empty }
  }

  it should "throw an NPE instead of the RuntimeException when a non-200 body is not JSON" in {
    // Pins current behavior: Unirest yields a null body when JSON parsing fails
    // (e.g. an HTML error page from a proxy), so the error-message construction
    // `response.getBody.toString` NPEs and callers lose all status/body context.
    queryResponder = _ => (502, "<html>Bad Gateway</html>")
    val ex = intercept[NullPointerException] {
      AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    }
    ex.getMessage should include("getBody()")
  }

  it should "throw a JSONException when a 200 response lacks the `results` key" in {
    // Realistic AsterixDB fatal shape: status 200 with errors but no results.
    queryResponder = _ => (200, """{"status":"fatal","errors":[{"msg":"boom"}]}""")
    val ex = intercept[JSONException] {
      AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    }
    ex.getMessage should include("""JSONObject["results"] not found""")
  }

  it should "treat any non-200 status as an error, even a successful 2xx like 201 or 204" in {
    // 201 with a well-formed `results` body: the status code alone routes it to
    // the error path, pinning the strict `== 200` check.
    queryResponder = _ => (201, """{"results":["x"]}""")
    val ex201 = intercept[RuntimeException] {
      AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    }
    ex201.getMessage should include("error status: Created")
    ex201.getMessage should include("""{"results":["x"]}""")
    // 204 with an empty body: Unirest parses the empty body as an empty JSON
    // object (not null), so this still reaches the RuntimeException path.
    queryResponder = _ => (204, "")
    val ex204 = intercept[RuntimeException] {
      AsterixDBConnUtil.queryAsterixDB(host, port, "SELECT 1;")
    }
    ex204.getMessage should include("error status: No Content")
    ex204.getMessage should include("error body: {}")
  }

  // ---------------------------------------------------------------------------
  // updateAsterixDBVersionMapping
  // ---------------------------------------------------------------------------

  "AsterixDBConnUtil.updateAsterixDBVersionMapping" should
    "store git.build.version for the host on HTTP 200" in {
    versionBody = versionJson("1.0.0-SNAPSHOT")
    AsterixDBConnUtil.updateAsterixDBVersionMapping(host, port)
    AsterixDBConnUtil.asterixDBVersionMapping.get(host) shouldBe Some("1.0.0-SNAPSHOT")
  }

  it should "silently leave the cache unset when /admin/version returns non-200" in {
    versionStatus = 404
    AsterixDBConnUtil.updateAsterixDBVersionMapping(host, port)
    AsterixDBConnUtil.asterixDBVersionMapping.contains(host) shouldBe false
  }

  it should "overwrite an existing entry for the host on a later successful probe" in {
    AsterixDBConnUtil.asterixDBVersionMapping += (host -> "0.9.5")
    versionBody = versionJson("0.9.9")
    AsterixDBConnUtil.updateAsterixDBVersionMapping(host, port)
    AsterixDBConnUtil.asterixDBVersionMapping.get(host) shouldBe Some("0.9.9")
  }

  it should "propagate a JSONException when a 200 body lacks git.build.version" in {
    // The silent tolerance only covers non-200 statuses; a 200 with an
    // unexpected body shape escapes as an exception and the cache stays unset.
    versionBody = """{"status":"ok"}"""
    val ex = intercept[JSONException] {
      AsterixDBConnUtil.updateAsterixDBVersionMapping(host, port)
    }
    ex.getMessage should include("""JSONObject["git.build.version"] not found""")
    AsterixDBConnUtil.asterixDBVersionMapping.contains(host) shouldBe false
  }

  it should "throw an NPE when a 200 version body is not valid JSON" in {
    // Same null-body-on-parse-failure behavior as the query path.
    versionBody = "not json at all"
    val ex = intercept[NullPointerException] {
      AsterixDBConnUtil.updateAsterixDBVersionMapping(host, port)
    }
    ex.getMessage should include("getBody()")
    AsterixDBConnUtil.asterixDBVersionMapping.contains(host) shouldBe false
  }

  // ---------------------------------------------------------------------------
  // fetchDataTypeFields
  // ---------------------------------------------------------------------------

  // Builds the /query/service response for one Metadata.`Datatype` row.
  private def datatypeRow(fields: (String, String)*): String = {
    val fieldJson = fields
      .map { case (name, tpe) => s"""{"FieldName":"$name","FieldType":"$tpe"}""" }
      .mkString(",")
    s"""{"results":[{"Fields":[$fieldJson]}]}"""
  }

  // Dispatches metadata queries by the datatype name embedded in the statement.
  private def metadataResponder(rows: Map[String, String]): String => (Int, String) = {
    val datatypeName = "DatatypeName = '([^']+)'".r
    statement =>
      datatypeName.findFirstMatchIn(statement).map(_.group(1)) match {
        case Some(name) => (200, rows.getOrElse(name, """{"results":[]}"""))
        case None       => (200, """{"results":[]}""")
      }
  }

  "AsterixDBConnUtil.fetchDataTypeFields" should
    "map flat record fields to their types without a prefix when parentName is empty" in {
    queryResponder = metadataResponder(
      Map("addressType" -> datatypeRow("zip" -> "string", "num" -> "int64", "名字" -> "string"))
    )
    val result = AsterixDBConnUtil.fetchDataTypeFields("addressType", "", host, port)
    result shouldBe Map("zip" -> "string", "num" -> "int64", "名字" -> "string")
  }

  it should "prefix every field with parentName and a dot when parentName is non-empty" in {
    queryResponder = metadataResponder(
      Map("addressType" -> datatypeRow("zip" -> "string", "num" -> "int64"))
    )
    val result = AsterixDBConnUtil.fetchDataTypeFields("addressType", "addr", host, port)
    result shouldBe Map("addr.zip" -> "string", "addr.num" -> "int64")
  }

  it should "recursively flatten nested record types into dot-separated field names" in {
    // tweetType.user is a usertype whose location is itself a geotype: the
    // flattening must recurse two levels deep and never emit the intermediate
    // record-typed fields themselves.
    queryResponder = metadataResponder(
      Map(
        "tweetType" -> datatypeRow("id" -> "int64", "user" -> "usertype"),
        "usertype" -> datatypeRow("screen_name" -> "string", "location" -> "geotype"),
        "geotype" -> datatypeRow("lat" -> "double", "lon" -> "double")
      )
    )
    val result = AsterixDBConnUtil.fetchDataTypeFields("tweetType", "", host, port)
    result shouldBe Map(
      "id" -> "int64",
      "user.screen_name" -> "string",
      "user.location.lat" -> "double",
      "user.location.lon" -> "double"
    )
    result should not contain key("user")
    result should not contain key("user.location")
  }

  it should "return an empty map when the datatype has no metadata row" in {
    queryResponder = metadataResponder(Map.empty)
    AsterixDBConnUtil.fetchDataTypeFields("unknownType", "", host, port) shouldBe empty
  }

  it should "silently drop a nested field whose child type metadata is missing (array types)" in {
    // `tags` claims a record type that has no metadata row (the shape AsterixDB
    // produces for array types); current behavior drops the field entirely.
    queryResponder = metadataResponder(
      Map("tweetType" -> datatypeRow("id" -> "int64", "tags" -> "arraytype"))
    )
    val result = AsterixDBConnUtil.fetchDataTypeFields("tweetType", "", host, port)
    result shouldBe Map("id" -> "int64")
  }

  it should "propagate the RuntimeException when the metadata query itself fails" in {
    // Only result parsing is failure-tolerant; a non-200 on the query bubbles up.
    queryResponder = _ => (500, """{"errors":"metadata unavailable"}""")
    val ex = intercept[RuntimeException] {
      AsterixDBConnUtil.fetchDataTypeFields("tweetType", "", host, port)
    }
    ex.getMessage should include("Send query to asterix failed")
  }

  it should "propagate a JSONException when a field row is missing FieldType" in {
    // The Try only wraps the Fields extraction; the per-field forEach runs
    // outside it, so a malformed field row escapes the failure tolerance.
    queryResponder = metadataResponder(
      Map("tweetType" -> """{"results":[{"Fields":[{"FieldName":"a"}]}]}""")
    )
    val ex = intercept[JSONException] {
      AsterixDBConnUtil.fetchDataTypeFields("tweetType", "", host, port)
    }
    ex.getMessage should include("""JSONObject["FieldType"] not found""")
  }

  it should "not recurse for a type name without a lowercase `type` substring" in {
    // The recursion heuristic is a case-sensitive contains("type"): "UserType"
    // does not match, so it is kept as a scalar and no child lookup is issued.
    queryResponder = metadataResponder(
      Map("tweetType" -> datatypeRow("u" -> "UserType"))
    )
    val result = AsterixDBConnUtil.fetchDataTypeFields("tweetType", "", host, port)
    result shouldBe Map("u" -> "UserType")
    recordedQueries.synchronized {
      recordedQueries.count(_("statement").contains("DatatypeName = 'UserType'")) shouldBe 0
    }
  }

  it should "return an empty map when the metadata row has no Fields key" in {
    // The shape of a primitive datatype's metadata row: a result row exists but
    // carries no Fields array, so getJSONArray fails inside the Try.
    queryResponder = metadataResponder(
      Map("int64" -> """{"results":[{"DatatypeName":"int64"}]}""")
    )
    AsterixDBConnUtil.fetchDataTypeFields("int64", "", host, port) shouldBe empty
  }

  it should "return an empty map for a record type with a zero-field Fields array" in {
    // Distinct from the missing-Fields case: this goes down the Success branch
    // and the forEach simply has nothing to iterate.
    queryResponder = metadataResponder(Map("emptyRecord" -> datatypeRow()))
    AsterixDBConnUtil.fetchDataTypeFields("emptyRecord", "", host, port) shouldBe empty
  }

  it should "consult only the first metadata row when results contains multiple rows" in {
    queryResponder = metadataResponder(
      Map(
        "tweetType" ->
          """{"results":[
            |{"Fields":[{"FieldName":"a","FieldType":"string"}]},
            |{"Fields":[{"FieldName":"b","FieldType":"int64"}]}
            |]}""".stripMargin
      )
    )
    val result = AsterixDBConnUtil.fetchDataTypeFields("tweetType", "", host, port)
    result shouldBe Map("a" -> "string")
  }
}
