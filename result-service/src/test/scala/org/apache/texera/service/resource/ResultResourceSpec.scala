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

package org.apache.texera.service.resource

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.dropwizard.auth.AuthValueFactoryProvider
import io.dropwizard.jackson.Jackson
import io.dropwizard.testing.junit5.ResourceExtension
import jakarta.ws.rs.core.{HttpHeaders, MediaType, Response}
import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.apache.texera.auth.{JwtAuth, JwtAuthFilter, SessionUser, UnauthorizedExceptionMapper}
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{OPERATOR_PORT_EXECUTIONS, WORKFLOW_EXECUTIONS}
import org.apache.texera.dao.jooq.generated.enums.{OutputModeEnum, PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  OperatorPortExecutionsDao,
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  OperatorPortExecutions,
  User,
  Workflow,
  WorkflowExecutions,
  WorkflowOfUser,
  WorkflowUserAccess,
  WorkflowVersion
}
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.URI
import scala.collection.mutable

/**
  * Sends requests through the Jersey pipeline production uses, against an embedded database. Only
  * the Iceberg table is faked: the platform CI jobs have no catalog to read one from.
  */
class ResultResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val Running: Short = 1
  private val Completed: Short = 3
  private val Failed: Short = 4

  private val schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("first_name", AttributeType.STRING),
    new Attribute("Last_Name", AttributeType.STRING)
  )

  private val rows: List[Tuple] = (0 until 25).toList.map { i =>
    Tuple.builder(schema).addSequentially(Array[Any](i, s"first-$i", s"last-$i")).build()
  }

  private class FakeDocument extends VirtualDocument[Tuple] {
    override def getURI: URI = new URI("vfs:///fake")
    override def clear(): Unit = ()
    override def getCount: Long = rows.length.toLong
    override def getRange(from: Int, until: Int, columns: Option[Seq[String]]): Iterator[Tuple] =
      rows.slice(from, until).map(_.getPartialTuple(columns.get.toList)).iterator
    override def getTableStatistics: Map[String, Map[String, Any]] =
      Map("id" -> Map("min" -> 0.0, "max" -> 24.0, "not_null_count" -> 25L))
  }

  private val opened = mutable.ListBuffer[URI]()
  private var openDocument: URI => (VirtualDocument[_], Option[Schema]) = _

  // Mirror production's mapper: ResultService registers DefaultScalaModule on Dropwizard's.
  private val mapper: ObjectMapper = Jackson.newObjectMapper().registerModule(DefaultScalaModule)

  private val resources: ResourceExtension = ResourceExtension
    .builder()
    .setMapper(mapper)
    .addProvider(classOf[JwtAuthFilter])
    .addProvider(classOf[UnauthorizedExceptionMapper])
    .addProvider(classOf[RolesAllowedDynamicFeature])
    .addProvider(new AuthValueFactoryProvider.Binder(classOf[SessionUser]))
    .addResource(new ResultResource(uri => openDocument(uri)))
    .build()

  private def newUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(s"$name@example.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val owner = newUser(1, "owner")
  private val grantee = newUser(2, "grantee")
  // Holds no grant: the workflow below is public, which is all this user has.
  private val publicViewer = newUser(3, "public-viewer")

  private var wid: Integer = _
  private var vid: Integer = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    resources.before()

    val configuration = getDSLContext.configuration()
    val userDao = new UserDao(configuration)
    Seq(owner, grantee, publicViewer).foreach(user => userDao.insert(user))

    val workflow = new Workflow
    workflow.setName("public-workflow")
    workflow.setContent("{}")
    workflow.setIsPublic(true)
    new WorkflowDao(configuration).insert(workflow)
    wid = workflow.getWid

    val ownership = new WorkflowOfUser
    ownership.setUid(owner.getUid)
    ownership.setWid(wid)
    new WorkflowOfUserDao(configuration).insert(ownership)

    val grant = new WorkflowUserAccess
    grant.setUid(grantee.getUid)
    grant.setWid(wid)
    grant.setPrivilege(PrivilegeEnum.READ)
    new WorkflowUserAccessDao(configuration).insert(grant)

    val version = new WorkflowVersion
    version.setWid(wid)
    version.setContent("{}")
    new WorkflowVersionDao(configuration).insert(version)
    vid = version.getVid
  }

  override protected def afterAll(): Unit = {
    resources.after()
    shutdownDB()
  }

  override protected def beforeEach(): Unit = {
    getDSLContext.deleteFrom(OPERATOR_PORT_EXECUTIONS).execute()
    getDSLContext.deleteFrom(WORKFLOW_EXECUTIONS).execute()
    opened.clear()
    openDocument = { uri =>
      opened += uri
      (new FakeDocument, Some(schema))
    }
  }

  private def insertExecution(startedBy: User, status: Short = Completed): Integer = {
    val execution = new WorkflowExecutions
    execution.setVid(vid)
    execution.setUid(startedBy.getUid)
    execution.setStatus(status)
    execution.setEnvironmentVersion("test")
    new WorkflowExecutionsDao(getDSLContext.configuration()).insert(execution)
    execution.getEid
  }

  private def insertRow(
      eid: Integer,
      port: GlobalPortIdentity,
      uri: String,
      mode: OutputModeEnum = OutputModeEnum.SET_SNAPSHOT,
      count: java.lang.Long = 25L
  ): Unit = {
    val row = new OperatorPortExecutions
    row.setWorkflowExecutionId(eid)
    row.setGlobalPortId(port.serializeAsString)
    row.setResultUri(uri)
    row.setOutputMode(mode)
    row.setResultCount(count)
    new OperatorPortExecutionsDao(getDSLContext.configuration()).insert(row)
  }

  private def globalPort(
      operatorId: String,
      port: Int = 0,
      internal: Boolean = false
  ): GlobalPortIdentity =
    GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity(operatorId), "main"),
      PortIdentity(port, internal)
    )

  private def resultUri(eid: Integer, port: GlobalPortIdentity): URI =
    VFSURIFactory.resultURI(
      VFSURIFactory.createPortBaseURI(
        WorkflowIdentity(wid.longValue()),
        ExecutionIdentity(eid.longValue()),
        port
      )
    )

  private def insertResult(
      eid: Integer,
      operatorId: String = "op",
      port: Int = 0,
      internal: Boolean = false,
      mode: OutputModeEnum = OutputModeEnum.SET_SNAPSHOT,
      count: java.lang.Long = 25L
  ): URI = {
    val globalPortId = globalPort(operatorId, port, internal)
    val uri = resultUri(eid, globalPortId)
    insertRow(eid, globalPortId, uri.toString, mode, count)
    uri
  }

  private def get(caller: User, path: String, query: (String, Any)*): Response =
    query
      .foldLeft(resources.target(path)) {
        case (target, (name, value)) => target.queryParam(name, value.toString)
      }
      .request(MediaType.APPLICATION_JSON)
      .header(HttpHeaders.AUTHORIZATION, s"Bearer ${JwtAuth.jwtToken(JwtAuth.jwtClaims(caller))}")
      .get()

  private def getJson(caller: User, path: String, query: (String, Any)*): JsonNode = {
    val response = get(caller, path, query: _*)
    response.getStatus shouldBe 200
    response.readEntity(classOf[JsonNode])
  }

  private def latestExecutionPath = s"/results/workflows/$wid/latest-execution"
  private def resultsPath(eid: Integer) = s"/results/executions/$eid"
  private def pagesPath(eid: Integer) = s"/results/executions/$eid/operators/op/pages"
  private def statisticsPath(eid: Integer) = s"/results/executions/$eid/operators/op/statistics"
  private def readPaths(eid: Integer) =
    Seq(resultsPath(eid), pagesPath(eid), statisticsPath(eid))

  private def columnNames(page: JsonNode): List[String] =
    (0 until page.get("schema").size).toList
      .map(page.get("schema").get(_).get("attributeName").asText)

  "GET latest-execution" should "return the caller's latest finished execution" in {
    insertExecution(owner, Completed)
    val latest = insertExecution(owner, Failed)
    insertExecution(owner, Running)
    insertExecution(grantee, Completed)

    getJson(owner, latestExecutionPath).get("eid").asInt shouldBe latest
  }

  it should "count completed, failed and killed executions as finished" in {
    Seq(3, 4, 5).foreach { status =>
      val eid = insertExecution(owner, status.toShort)
      getJson(owner, latestExecutionPath).get("eid").asInt shouldBe eid
    }
  }

  it should "answer 404 while the caller has no finished execution" in {
    Seq(0, 1, 2).foreach(status => insertExecution(owner, status.toShort))
    insertExecution(grantee, Completed)

    get(owner, latestExecutionPath).getStatus shouldBe 404
  }

  "Reading an execution" should "be allowed for the user who started it" in {
    // The public viewer started this one, so its results are theirs despite holding no grant.
    val eid = insertExecution(publicViewer)
    insertResult(eid)

    readPaths(eid).foreach(path => withClue(path)(get(publicViewer, path).getStatus shouldBe 200))
  }

  it should "be allowed for the owner and anyone granted the workflow" in {
    val eid = insertExecution(publicViewer)
    insertResult(eid)

    for (caller <- Seq(owner, grantee); path <- readPaths(eid)) {
      withClue(s"${caller.getName} on $path")(get(caller, path).getStatus shouldBe 200)
    }
  }

  it should "be refused for a user who only sees the workflow because it is public" in {
    val eid = insertExecution(owner)
    insertResult(eid)

    readPaths(eid).foreach(path => withClue(path)(get(publicViewer, path).getStatus shouldBe 403))
    opened shouldBe empty
  }

  it should "give an execution that does not exist the same refusal" in {
    readPaths(Integer.MAX_VALUE).foreach { path =>
      withClue(path)(get(owner, path).getStatus shouldBe 403)
    }
  }

  it should "be refused without a token" in {
    val eid = insertExecution(owner)
    insertResult(eid)

    (latestExecutionPath +: readPaths(eid)).foreach { path =>
      withClue(path) {
        resources.target(path).request(MediaType.APPLICATION_JSON).get().getStatus shouldBe 401
      }
    }
  }

  "GET an execution's results" should "list its operators from the database alone" in {
    val eid = insertExecution(owner)
    insertResult(eid, "b", count = 7L)
    insertResult(eid, "a", port = 1, mode = OutputModeEnum.SET_DELTA, count = null)
    insertResult(eid, "a", mode = OutputModeEnum.SINGLE_SNAPSHOT, count = null)
    insertResult(eid, "from-before-the-columns", mode = null, count = null)

    getJson(owner, resultsPath(eid)) shouldBe mapper.readTree(
      """[
        |{"operatorId":"a","portIndex":0,"outputMode":"SINGLE_SNAPSHOT","rowCount":null},
        |{"operatorId":"a","portIndex":1,"outputMode":"SET_DELTA","rowCount":null},
        |{"operatorId":"b","portIndex":0,"outputMode":"SET_SNAPSHOT","rowCount":7},
        |{"operatorId":"from-before-the-columns","portIndex":0,"outputMode":null,"rowCount":null}
        |]""".stripMargin
    )
    opened shouldBe empty
  }

  it should "order an operator's ports by number" in {
    val eid = insertExecution(owner)
    // As text, which is how the table's key orders them, port 10 comes before port 2.
    insertResult(eid, port = 10)
    insertResult(eid, port = 2)

    val results = getJson(owner, resultsPath(eid))
    (0 until results.size).map(results.get(_).get("portIndex").asInt) shouldBe Seq(2, 10)
  }

  it should "leave out internal ports and rows with no stored result" in {
    val eid = insertExecution(owner)
    insertResult(eid, "internal", internal = true)
    insertRow(eid, globalPort("empty-uri"), "")
    insertRow(eid, globalPort("null-uri"), null)

    getJson(owner, resultsPath(eid)).size shouldBe 0
  }

  "GET a page" should "return the requested rows with the schema and the full row count" in {
    val eid = insertExecution(owner)
    insertResult(eid)

    val page = getJson(owner, pagesPath(eid), "pageIndex" -> 2, "pageSize" -> 10)

    page.get("table").size shouldBe 10
    page.get("table").get(0) shouldBe mapper.readTree(
      """{"id":10,"first_name":"first-10","Last_Name":"last-10"}"""
    )
    page.get("schema") shouldBe mapper.readTree(
      """[
        |{"attributeName":"id","attributeType":"integer"},
        |{"attributeName":"first_name","attributeType":"string"},
        |{"attributeName":"Last_Name","attributeType":"string"}
        |]""".stripMargin
    )
    page.get("rowCount").asLong shouldBe 25
  }

  it should "default to the first ten rows of port 0" in {
    val eid = insertExecution(owner)
    val port0 = insertResult(eid)
    insertResult(eid, port = 1)

    val page = getJson(owner, pagesPath(eid))

    page.get("table").size shouldBe 10
    page.get("table").get(0).get("id").asInt shouldBe 0
    opened.toList shouldBe List(port0)
  }

  it should "read the port it is asked for" in {
    val eid = insertExecution(owner)
    insertResult(eid)
    val port1 = insertResult(eid, port = 1)

    getJson(owner, pagesPath(eid), "port" -> 1)

    opened.toList shouldBe List(port1)
  }

  it should "stop at the end of the result" in {
    val eid = insertExecution(owner)
    insertResult(eid)

    getJson(owner, pagesPath(eid), "pageIndex" -> 3, "pageSize" -> 10).get("table").size shouldBe 5
  }

  it should "keep the schema on a page past the end" in {
    val eid = insertExecution(owner)
    insertResult(eid)

    val page = getJson(owner, pagesPath(eid), "pageIndex" -> 9, "pageSize" -> 10)

    page.get("table").size shouldBe 0
    columnNames(page) shouldBe List("id", "first_name", "Last_Name")
    page.get("rowCount").asLong shouldBe 25
  }

  it should "apply the column offset and limit" in {
    val eid = insertExecution(owner)
    insertResult(eid)

    // With the limit left at its default too: offset plus limit does not fit in an Int.
    columnNames(getJson(owner, pagesPath(eid), "columnOffset" -> 1)) shouldBe
      List("first_name", "Last_Name")
    columnNames(getJson(owner, pagesPath(eid), "columnOffset" -> 1, "columnLimit" -> 1)) shouldBe
      List("first_name")
  }

  it should "search the columns ignoring case, before the offset and limit" in {
    val eid = insertExecution(owner)
    insertResult(eid)

    columnNames(getJson(owner, pagesPath(eid), "columnSearch" -> "NAME")) shouldBe
      List("first_name", "Last_Name")
    columnNames(
      getJson(owner, pagesPath(eid), "columnSearch" -> "name", "columnOffset" -> 1)
    ) shouldBe
      List("Last_Name")
    columnNames(getJson(owner, pagesPath(eid), "columnSearch" -> "")) shouldBe
      List("id", "first_name", "Last_Name")
  }

  it should "refuse a page or column range out of bounds" in {
    val eid = insertExecution(owner)
    insertResult(eid)

    Seq[(String, Any)](
      "pageIndex" -> 0,
      "pageSize" -> 0,
      "pageSize" -> 1001,
      "columnOffset" -> -1,
      "columnLimit" -> -1
    ).foreach { param =>
      withClue(param)(get(owner, pagesPath(eid), param).getStatus shouldBe 400)
    }
    // The end of this page does not fit in an Int.
    get(owner, pagesPath(eid), "pageIndex" -> Int.MaxValue, "pageSize" -> 2).getStatus shouldBe 400
    opened shouldBe empty

    get(owner, pagesPath(eid), "pageSize" -> 1000).getStatus shouldBe 200
  }

  it should "answer 404 for an operator with no result" in {
    val eid = insertExecution(owner)
    insertResult(eid, "other-op")

    get(owner, pagesPath(eid)).getStatus shouldBe 404
  }

  it should "answer 404 for a table that is no longer stored" in {
    val eid = insertExecution(owner)
    insertResult(eid)
    openDocument = _ => throw new IllegalArgumentException("No storage is found for the given URI")

    get(owner, pagesPath(eid)).getStatus shouldBe 404
  }

  it should "not pass off any other failure to open the table as a missing result" in {
    val eid = insertExecution(owner)
    insertResult(eid)
    openDocument = _ => throw new IllegalArgumentException("Unsupported catalog type: nope")

    get(owner, pagesPath(eid)).getStatus shouldBe 500
  }

  it should "answer 503 for a result in a warehouse it cannot read" in {
    val eid = insertExecution(owner)
    val port = globalPort("op")
    // An unresolvable warehouse is refused whether or not per-user warehouses are enabled.
    val uri = resultUri(eid, port).toString.replace("vfs:///", "vfs:///wh/a%2Fb/")
    insertRow(eid, port, uri)

    get(owner, pagesPath(eid)).getStatus shouldBe 503
    opened shouldBe empty
  }

  "GET statistics" should "return the table's per-column statistics" in {
    val eid = insertExecution(owner)
    insertResult(eid)

    getJson(owner, statisticsPath(eid)) shouldBe mapper.readTree(
      """{"id":{"min":0.0,"max":24.0,"not_null_count":25}}"""
    )
  }

  it should "answer 404 for an operator with no result" in {
    val eid = insertExecution(owner)

    get(owner, statisticsPath(eid)).getStatus shouldBe 404
  }
}
