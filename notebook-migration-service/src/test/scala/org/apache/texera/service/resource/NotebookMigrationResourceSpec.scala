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

import jakarta.ws.rs.core.Response
import org.apache.texera.auth.SessionUser
import io.fabric8.kubernetes.client.KubernetesClientException
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.service.util.{
  JupyterEndpointResolver,
  JupyterEndpoints,
  JupyterKubernetesClient,
  JupyterProvisioner,
  JupyterTokenDeriver
}
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.Notebook.NOTEBOOK
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.UserJupyter.USER_JUPYTER
import org.apache.texera.dao.jooq.generated.tables.Workflow.WORKFLOW
import org.apache.texera.dao.jooq.generated.tables.WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING
import org.apache.texera.dao.jooq.generated.tables.WorkflowUserAccess.WORKFLOW_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.WorkflowVersion.WORKFLOW_VERSION
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  UserJupyterDao,
  WorkflowDao,
  WorkflowUserAccessDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  UserJupyter,
  Workflow,
  WorkflowUserAccess,
  WorkflowVersion
}
import org.jooq.JSONB
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import com.sun.net.httpserver.HttpServer

import java.net.InetSocketAddress

import scala.jdk.CollectionConverters._
import java.sql.Timestamp
import java.util.UUID

class NotebookMigrationResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Randomise the seeded wid so a parallel run of unrelated specs that happen
  // to seed the same id wouldn't collide on the embedded postgres.
  private val testWid = 9000 + scala.util.Random.nextInt(1000)
  private val writerEmail = s"nbms_writer_$testWid@example.com"
  private val readerEmail = s"nbms_reader_$testWid@example.com"

  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var userDao: UserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _
  private var seededVid: Integer = _
  private var writerUid: Integer = _ // holds WRITE access to testWid
  private var readerUid: Integer = _ // holds READ access to testWid

  // Method and path of the last /api/contents request the fake Jupyter saw, so a test can pin
  // the verb and URL a Jupyter call uses. A var is safe here because the spec runs sequentially.
  private var lastContentsRequest: Option[(String, String)] = None

  private val sampleNotebook =
    """{"cells":[{"cell_type":"code","metadata":{},"source":"print(1)"}]}"""
  private val sampleMapping =
    """{"operator_to_cell":{},"cell_to_operator":{}}"""

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()
  override protected def afterAll(): Unit = closeConnectionPool()

  override protected def beforeEach(): Unit = {
    val cfg = getDSLContext.configuration()
    workflowDao = new WorkflowDao(cfg)
    workflowVersionDao = new WorkflowVersionDao(cfg)
    userDao = new UserDao(cfg)
    workflowUserAccessDao = new WorkflowUserAccessDao(cfg)
    lastContentsRequest = None
    cleanup()

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName(s"wf_${UUID.randomUUID().toString.substring(0, 8)}")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(workflow)

    val version = new WorkflowVersion
    version.setWid(testWid)
    version.setContent("{}")
    version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflowVersionDao.insert(version)
    seededVid = version.getVid

    // One user with WRITE access (the happy path) and one with only READ
    // access, so the write-access gate can be exercised both ways.
    writerUid = insertUser("nbms_writer", writerEmail)
    readerUid = insertUser("nbms_reader", readerEmail)
    insertAccess(writerUid, PrivilegeEnum.WRITE)
    insertAccess(readerUid, PrivilegeEnum.READ)
  }

  override protected def afterEach(): Unit = cleanup()

  private def insertUser(name: String, email: String): Integer = {
    val user = new User
    user.setName(name)
    user.setEmail(email)
    user.setRole(UserRoleEnum.REGULAR)
    userDao.insert(user)
    user.getUid
  }

  private def insertAccess(uid: Integer, privilege: PrivilegeEnum): Unit = {
    val access = new WorkflowUserAccess
    access.setWid(testWid)
    access.setUid(uid)
    access.setPrivilege(privilege)
    workflowUserAccessDao.insert(access)
  }

  private def cleanup(): Unit = {
    // Delete children before parents. workflow_user_access and notebook cascade
    // on workflow delete, but explicit deletes keep state observable across tests
    // and avoid depending on cascade ordering.
    getDSLContext.deleteFrom(WORKFLOW_NOTEBOOK_MAPPING).execute()
    getDSLContext.deleteFrom(NOTEBOOK).execute()
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(testWid))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWid)).execute()
    getDSLContext.deleteFrom(USER_JUPYTER).execute()
    getDSLContext.deleteFrom(USER).where(USER.EMAIL.in(writerEmail, readerEmail)).execute()
  }

  // The endpoints resolve the vid server-side (store) or ignore it (fetch), so the
  // client sends only the wid — no vid field, matching the real frontend requests.
  private def storePayload(
      notebook: String = sampleNotebook,
      mapping: String = sampleMapping
  ): String =
    s"""{"wid": $testWid, "notebook": $notebook, "mapping": $mapping}"""

  private def fetchPayload(): String =
    s"""{"wid": $testWid}"""

  private def deletePayload(): String =
    s"""{"wid": $testWid}"""

  private def deleteNotebookPayload(name: String = "notebook.ipynb"): String =
    s"""{"notebookName": "$name"}"""

  private val resource = new NotebookMigrationResource()

  private def sessionUser(uid: Integer): SessionUser = {
    val u = new User
    u.setUid(uid)
    new SessionUser(u)
  }

  // Runs `test` with a stub Jupyter server on localhost:9100 (the configured
  // jupyter URL): GET /api returns 200 so isJupyterAvailable() passes, and PUT
  // /api/contents/<name> returns `contentsStatus`. Lets the HTTP success/failure
  // paths run without a real Jupyter. Sequential test execution (Tags.limit) keeps
  // this from colliding with the "unreachable" test, which needs the port free.
  private def withFakeJupyter(contentsStatus: Int, apiStatus: Int = 200)(test: => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("localhost", 9100), 0)
    server.createContext(
      "/api",
      (exchange: com.sun.net.httpserver.HttpExchange) => {
        exchange.getRequestBody.readAllBytes()
        val body = """{"version":"2.7.0"}""".getBytes("UTF-8")
        exchange.sendResponseHeaders(apiStatus, body.length)
        val os = exchange.getResponseBody
        os.write(body)
        os.close()
      }
    )
    // Longest-prefix match means /api/contents/... routes here, not to /api.
    server.createContext(
      "/api/contents",
      (exchange: com.sun.net.httpserver.HttpExchange) => {
        exchange.getRequestBody.readAllBytes()
        lastContentsRequest = Some((exchange.getRequestMethod, exchange.getRequestURI.getPath))
        if (contentsStatus == 204) {
          // 204 carries no body, so send the headers with a -1 length.
          exchange.sendResponseHeaders(contentsStatus, -1)
        } else {
          val body = "{}".getBytes("UTF-8")
          exchange.sendResponseHeaders(contentsStatus, body.length)
          val os = exchange.getResponseBody
          os.write(body)
          os.close()
        }
      }
    )
    server.start()
    try test
    finally server.stop(0)
  }

  // -- storeNotebookAndMapping ------------------------------------------------

  "storeNotebookAndMapping" should "insert one notebook and one mapping tied to the workflow version" in {
    val response = NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)
    response.getStatus shouldBe Response.Status.OK.getStatusCode

    getDSLContext.fetchCount(NOTEBOOK) shouldBe 1
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 1

    val notebookRow = getDSLContext.selectFrom(NOTEBOOK).fetchOne()
    notebookRow.get(NOTEBOOK.WID) shouldBe testWid

    val mappingRow = getDSLContext.selectFrom(WORKFLOW_NOTEBOOK_MAPPING).fetchOne()
    mappingRow.get(WORKFLOW_NOTEBOOK_MAPPING.WID) shouldBe testWid
    // vid is resolved server-side to the workflow's latest version (the only seeded one here).
    mappingRow.get(WORKFLOW_NOTEBOOK_MAPPING.VID) shouldBe seededVid
    // The mapping row must reference the just-inserted notebook by its returned nid.
    mappingRow.get(WORKFLOW_NOTEBOOK_MAPPING.NID) shouldBe notebookRow.get(NOTEBOOK.NID)
  }

  it should "round-trip notebook and mapping JSON content through the JSONB columns" in {
    val notebook =
      """{"cells":[{"cell_type":"code","metadata":{"uuid":"abc-123"},"source":"x = 1"}]}"""
    val mapping =
      """{"operator_to_cell":{"op1":["cell1"]},"cell_to_operator":{"cell1":["op1"]}}"""

    NotebookMigrationResource.storeNotebookAndMapping(storePayload(notebook, mapping), writerUid)

    val storedNotebookJson =
      getDSLContext
        .selectFrom(NOTEBOOK)
        .fetchOne()
        .get(NOTEBOOK.NOTEBOOK_)
        .asInstanceOf[JSONB]
        .data()
    val storedMappingJson =
      getDSLContext
        .selectFrom(WORKFLOW_NOTEBOOK_MAPPING)
        .fetchOne()
        .get(WORKFLOW_NOTEBOOK_MAPPING.MAPPING)
        .asInstanceOf[JSONB]
        .data()

    // Use whitespace-agnostic substring checks — postgres canonicalises JSONB
    // text on the way out, so exact-string compare against the input would be
    // fragile across postgres versions.
    storedNotebookJson should include("\"abc-123\"")
    storedNotebookJson should include("x = 1")
    storedMappingJson should include("\"op1\"")
    storedMappingJson should include("\"cell1\"")
  }

  it should "ignore a client-supplied vid and anchor the mapping to the workflow's latest version" in {
    // The vid FK is resolved server-side to MAX(workflow_version.vid) for the wid, so a
    // stale or bogus vid in the request body must never reach the mapping row. This pins
    // the fix for the old hardcoded vid=1 behaviour.
    val payload =
      s"""{"wid": $testWid, "vid": 999999, "notebook": $sampleNotebook, "mapping": $sampleMapping}"""
    val response = NotebookMigrationResource.storeNotebookAndMapping(payload, writerUid)
    response.getStatus shouldBe Response.Status.OK.getStatusCode

    val mappingRow = getDSLContext.selectFrom(WORKFLOW_NOTEBOOK_MAPPING).fetchOne()
    mappingRow.get(WORKFLOW_NOTEBOOK_MAPPING.VID) shouldBe seededVid
  }

  it should "return 400 and store nothing when the workflow has no version to anchor the mapping" in {
    // The mapping's vid FK needs a real workflow_version row. With none, the store must
    // fail cleanly with a 400 before any insert, not a 500 from the FK constraint.
    getDSLContext.deleteFrom(WORKFLOW_VERSION).where(WORKFLOW_VERSION.WID.eq(testWid)).execute()

    val response = NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)
    response.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
    getDSLContext.fetchCount(NOTEBOOK) shouldBe 0
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 0
  }

  it should "return 409 Conflict on a second store for a workflow that already has a notebook" in {
    // notebook.wid is UNIQUE — one notebook per workflow. The second store must be
    // rejected with an explicit 409 (not a 500 from the constraint violation), and
    // must not add a second notebook or mapping row.
    val first = NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)
    first.getStatus shouldBe Response.Status.OK.getStatusCode

    val second = NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)
    second.getStatus shouldBe Response.Status.CONFLICT.getStatusCode

    getDSLContext.fetchCount(NOTEBOOK) shouldBe 1
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 1
  }

  // -- fetchNotebookAndMapping ------------------------------------------------

  "fetchNotebookAndMapping" should "return exists=false when no notebook is stored for the workflow" in {
    val response = NotebookMigrationResource.fetchNotebookAndMapping(fetchPayload(), writerUid)
    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getEntity.toString should include("\"exists\": false")
  }

  it should "return exists=true with the stored notebook and mapping when a row exists" in {
    NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)

    val entity =
      NotebookMigrationResource
        .fetchNotebookAndMapping(fetchPayload(), writerUid)
        .getEntity
        .toString
    entity should include("\"exists\": true")
    entity should include("\"notebook\":")
    entity should include("\"mapping\":")
  }

  it should "return the stored notebook content for the workflow on fetch" in {
    // notebook.wid is UNIQUE — one notebook per workflow — so the endpoint's
    // orderBy(NID.desc).limit(1) resolves to that single row. This pins the
    // workflow-reopen path: after a store, fetch must return that notebook's content.
    val notebook =
      """{"cells":[{"cell_type":"code","metadata":{},"source":"v1"}]}"""

    NotebookMigrationResource.storeNotebookAndMapping(
      storePayload(notebook, sampleMapping),
      writerUid
    )

    val entity =
      NotebookMigrationResource
        .fetchNotebookAndMapping(fetchPayload(), writerUid)
        .getEntity
        .toString
    entity should include("\"v1\"")
  }

  it should "return the notebook regardless of the workflow's current version" in {
    // The mapping is stored under the version present at store time; the workflow may then
    // advance to a newer version. Fetch keys on wid alone, so the notebook still reattaches.
    // This mirrors the reopen-after-edit path.
    NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)

    val newerVersion = new WorkflowVersion
    newerVersion.setWid(testWid)
    newerVersion.setContent("{}")
    newerVersion.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflowVersionDao.insert(newerVersion)
    newerVersion.getVid.intValue() should be > seededVid.intValue()

    val entity =
      NotebookMigrationResource
        .fetchNotebookAndMapping(fetchPayload(), writerUid)
        .getEntity
        .toString
    entity should include("\"exists\": true")
  }

  // -- deleteNotebookAndMapping -----------------------------------------------

  "deleteNotebookAndMapping" should "remove the notebook and cascade to its mapping, reporting deleted=1" in {
    NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)
    getDSLContext.fetchCount(NOTEBOOK) shouldBe 1
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 1

    val response = NotebookMigrationResource.deleteNotebookAndMapping(deletePayload(), writerUid)
    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getEntity.toString should include("\"deleted\":1")

    // Deleting the notebook row cascades to workflow_notebook_mapping via the FK.
    getDSLContext.fetchCount(NOTEBOOK) shouldBe 0
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 0
  }

  it should "be idempotent, returning success with deleted=0 when nothing is stored" in {
    val response = NotebookMigrationResource.deleteNotebookAndMapping(deletePayload(), writerUid)
    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getEntity.toString should include("\"deleted\":0")
  }

  it should "return 403 Forbidden and delete nothing when the user lacks write access" in {
    NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)

    // readerUid holds only READ access; delete requires WRITE.
    NotebookMigrationResource
      .deleteNotebookAndMapping(deletePayload(), readerUid)
      .getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode

    getDSLContext.fetchCount(NOTEBOOK) shouldBe 1
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 1
  }

  it should "return 400 when the request body is malformed JSON" in {
    // Malformed input is a client error, caught by parseBody before the generic 500 handler.
    resource
      .deleteNotebookAndMapping("not json", sessionUser(writerUid))
      .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
  }

  // -- wid validation ---------------------------------------------------------

  "store/fetch/delete" should "return 400 Bad Request when 'wid' is missing from the body" in {
    // A missing wid must be a client error, not a 500 from the null.asInt() NPE.
    val noWid = """{"notebook": {}, "mapping": {}}"""
    NotebookMigrationResource
      .storeNotebookAndMapping(noWid, writerUid)
      .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
    NotebookMigrationResource
      .fetchNotebookAndMapping(noWid, writerUid)
      .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
    NotebookMigrationResource
      .deleteNotebookAndMapping("""{}""", writerUid)
      .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode

    getDSLContext.fetchCount(NOTEBOOK) shouldBe 0
  }

  it should "return 400 Bad Request when 'wid' is not an integer" in {
    // A non-integer wid must be rejected rather than silently coerced to 0 by asInt().
    val badWid = """{"wid": "not-an-int"}"""
    NotebookMigrationResource
      .storeNotebookAndMapping(badWid, writerUid)
      .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
    NotebookMigrationResource
      .fetchNotebookAndMapping(badWid, writerUid)
      .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
    NotebookMigrationResource
      .deleteNotebookAndMapping(badWid, writerUid)
      .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode

    getDSLContext.fetchCount(NOTEBOOK) shouldBe 0
  }

  // -- workflow write-access enforcement --------------------------------------

  "store/fetch" should "return 403 Forbidden when the user lacks write access to the workflow" in {
    // readerUid holds only READ access; the endpoints require WRITE, so both must
    // be rejected with a 403 and no notebook may be written.
    NotebookMigrationResource
      .storeNotebookAndMapping(storePayload(), readerUid)
      .getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode

    NotebookMigrationResource
      .fetchNotebookAndMapping(fetchPayload(), readerUid)
      .getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode

    getDSLContext.fetchCount(NOTEBOOK) shouldBe 0
  }

  // -- JAX-RS resource class (@Auth wrappers + Jupyter reachability) ----------

  "the resource class endpoints" should "store and fetch via the authenticated class methods for a write-access user" in {
    resource
      .storeNotebookAndMapping(storePayload(), sessionUser(writerUid))
      .getStatus shouldBe Response.Status.OK.getStatusCode

    resource
      .fetchNotebookAndMapping(fetchPayload(), sessionUser(writerUid))
      .getEntity
      .toString should include("\"exists\": true")
  }

  it should "reject the class methods for a read-only user with 403" in {
    resource
      .storeNotebookAndMapping(storePayload(), sessionUser(readerUid))
      .getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
    resource
      .fetchNotebookAndMapping(fetchPayload(), sessionUser(readerUid))
      .getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "return 500 from the Jupyter endpoints when the Jupyter server is unreachable" in {
    // No Jupyter server runs in the unit-test environment, so isJupyterAvailable
    // fails the connection and these endpoints surface a 500 rather than crashing.
    val user = sessionUser(writerUid)
    val validNotebook = """{"notebookName": "notebook.ipynb", "notebookData": {"cells": []}}"""

    resource.setNotebook(validNotebook, user).getStatus shouldBe 500
    resource.getJupyterURL(user).getStatus shouldBe 500
    resource.getJupyterIframeURL(null, user).getStatus shouldBe 500
    resource.deleteNotebook(deleteNotebookPayload(), user).getStatus shouldBe 500
  }

  it should "return 400 when the request body is malformed JSON" in {
    // Malformed input is a client error: parseBody rejects it before any downstream work.
    val user = sessionUser(writerUid)
    val badRequest = Response.Status.BAD_REQUEST.getStatusCode
    resource.setNotebook("not json", user).getStatus shouldBe badRequest
    resource.storeNotebookAndMapping("not json", user).getStatus shouldBe badRequest
    resource.fetchNotebookAndMapping("not json", user).getStatus shouldBe badRequest
  }

  it should "upload the notebook and return success when Jupyter accepts it" in {
    withFakeJupyter(contentsStatus = 201) {
      val body = """{"notebookName": "notebook.ipynb", "notebookData": {"cells": []}}"""
      val resp = resource.setNotebook(body, sessionUser(writerUid))
      resp.getStatus shouldBe Response.Status.OK.getStatusCode
      resp.getEntity.toString should include("success")
    }
  }

  it should "return 500 when Jupyter rejects the notebook upload" in {
    withFakeJupyter(contentsStatus = 500) {
      val body = """{"notebookName": "notebook.ipynb", "notebookData": {"cells": []}}"""
      resource.setNotebook(body, sessionUser(writerUid)).getStatus shouldBe 500
    }
  }

  it should "return the Jupyter URL and iframe URL when the server is reachable" in {
    withFakeJupyter(contentsStatus = 201) {
      val urlResp = resource.getJupyterURL(sessionUser(writerUid))
      urlResp.getStatus shouldBe Response.Status.OK.getStatusCode
      urlResp.getEntity.toString should include("localhost:9100")

      val iframeResp = resource.getJupyterIframeURL(null, sessionUser(writerUid))
      iframeResp.getStatus shouldBe Response.Status.OK.getStatusCode
      iframeResp.getEntity.toString should include("/notebooks/work/notebook.ipynb")
    }
  }

  // -- internal vs public Jupyter URL ----------------------------------------
  // The rest of the suite runs with the configured default, where both URLs are
  // localhost:9100, so it cannot tell the two apart. These pin the split itself.

  // Reachable stub for what the service dials; an unroutable address for what the browser
  // gets. 192.0.2.0/24 is TEST-NET-1 (RFC 5737) and routes nowhere, so a call that wrongly
  // dials the public URL fails rather than silently passing. Numeric on purpose: a hostname
  // would go through the resolver, which setConnectTimeout does not bound.
  private val splitEndpoints = JupyterEndpoints(
    internalUrl = "http://localhost:9100",
    publicUrl = "http://192.0.2.1:1234",
    token = "texera"
  )

  // -- per-user resolution ----------------------------------------------------

  // Registers a Jupyter for `uid`, standing in for a provisioned pod.
  private def registerJupyter(
      uid: Integer,
      internalUrl: String = "http://localhost:9100",
      publicUrl: String = "http://192.0.2.1:1234"
  ): Unit = {
    val row = new UserJupyter
    row.setUid(uid)
    row.setInternalUrl(internalUrl)
    row.setPublicUrl(publicUrl)
    new UserJupyterDao(getDSLContext.configuration()).insert(row)
  }

  private val specSecret = "resolver-spec-secret"

  "JupyterEndpointResolver" should "resolve every user to the configured Jupyter while the feature is off" in {
    // How single-node and local dev run: one shared JupyterLab, no registry rows.
    JupyterEndpointResolver.resolve(writerUid, jupyterEnabled = false) shouldBe Some(
      JupyterEndpoints.configured
    )
  }

  it should "resolve a registered user to their own Jupyter" in {
    registerJupyter(writerUid, internalUrl = "http://jupyter-1:8888")
    val resolved =
      JupyterEndpointResolver.resolve(writerUid, jupyterEnabled = true, tokenSecret = specSecret)
    resolved.map(_.internalUrl) shouldBe Some("http://jupyter-1:8888")
    resolved.map(_.publicUrl) shouldBe Some("http://192.0.2.1:1234")
  }

  it should "return None for an unregistered user rather than falling back to the shared Jupyter" in {
    // The isolation property: falling back here would hand an unprovisioned user somebody
    // else's notebooks, which is the whole point of resolving per user.
    JupyterEndpointResolver.resolve(
      writerUid,
      jupyterEnabled = true,
      tokenSecret = specSecret
    ) shouldBe None
  }

  it should "never return one user's Jupyter to another" in {
    registerJupyter(writerUid, internalUrl = "http://jupyter-writer:8888")
    JupyterEndpointResolver
      .resolve(writerUid, jupyterEnabled = true, tokenSecret = specSecret)
      .map(_.internalUrl) shouldBe Some("http://jupyter-writer:8888")
    JupyterEndpointResolver.resolve(
      readerUid,
      jupyterEnabled = true,
      tokenSecret = specSecret
    ) shouldBe None
  }

  it should "derive the registered user's token rather than reading one from the row" in {
    // No token column exists, so the resolver has to rebuild it from the uid.
    registerJupyter(writerUid)
    JupyterEndpointResolver
      .resolve(writerUid, jupyterEnabled = true, tokenSecret = specSecret)
      .map(_.token) shouldBe Some(JupyterTokenDeriver.derive(writerUid, specSecret))
  }

  it should "give two registered users different tokens" in {
    registerJupyter(writerUid)
    registerJupyter(readerUid)
    val writerToken = JupyterEndpointResolver
      .resolve(writerUid, jupyterEnabled = true, tokenSecret = specSecret)
      .map(_.token)
    val readerToken = JupyterEndpointResolver
      .resolve(readerUid, jupyterEnabled = true, tokenSecret = specSecret)
      .map(_.token)
    writerToken should not be readerToken
  }

  "the resource class" should "report Jupyter unavailable when the caller has none" in {
    // What an unprovisioned user gets: never a fall back to somebody else's Jupyter.
    val response = new NotebookMigrationResource()
      .respondWith(None, _ => fail("must not call through without a Jupyter"))
    response.getStatus shouldBe 500
    response.getEntity.toString should include("Cannot connect to Jupyter server")
  }

  it should "call through to the endpoint when the caller has a Jupyter" in {
    val response = new NotebookMigrationResource()
      .respondWith(Some(splitEndpoints), jupyter => Response.ok(jupyter.internalUrl).build())
    response.getEntity.toString shouldBe "http://localhost:9100"
  }

  // -- provisioning -----------------------------------------------------------

  // Records what would have been asked of Kubernetes, so the provisioning logic runs without
  // a cluster. Subclassing rather than mocking keeps the real naming and addressing.
  private class StubKubernetes extends JupyterKubernetesClient(null) {
    var created: List[(Int, String)] = Nil
    var deleted: List[Int] = Nil
    var alreadyExists = false
    var failCreate = false
    var failDelete = false
    // Distinct from failCreate: 409 means another request won the race, which is success.
    var createConflicts = false
    // Polls the pod must survive after deletePod before it reports gone, so the spec can model
    // the Terminating window a real cluster has. 0 deletes synchronously.
    var terminatingPolls = 0
    override def podExists(uid: Int): Boolean =
      if (terminatingPolls > 0) { terminatingPolls -= 1; true }
      else alreadyExists
    override def createPod(uid: Int, token: String) = {
      if (createConflicts) throw new KubernetesClientException("already exists", 409, null)
      if (failCreate) throw new RuntimeException("cluster refused the pod")
      created ::= ((uid, token))
      null
    }
    override def deletePod(uid: Int): Unit = {
      deleted ::= uid
      if (failDelete) throw new RuntimeException("pod already gone")
      alreadyExists = false
    }
  }

  // Short windows so the "never ready" path does not sit in a real timeout.
  private def provisionerFor(
      kubernetes: JupyterKubernetesClient,
      accepts: (String, String) => Boolean,
      publicUrlTemplate: String = "",
      maxConcurrentProvisions: Int = 4
  ) =
    new JupyterProvisioner(
      kubernetes,
      accepts,
      publicUrlTemplate,
      readinessTimeoutMillis = 50,
      readinessPollMillis = 10,
      maxConcurrentProvisions
    )

  private def registeredUids(): List[Integer] =
    getDSLContext
      .select(USER_JUPYTER.UID)
      .from(USER_JUPYTER)
      .fetchInto(classOf[Integer])
      .asScala
      .toList

  "JupyterProvisioner.ensure" should "return the configured Jupyter and start nothing while the feature is off" in {
    val kubernetes = new StubKubernetes
    val result =
      provisionerFor(kubernetes, (_, _) => true).ensure(writerUid, jupyterEnabled = false)
    result shouldBe Some(JupyterEndpoints.configured)
    kubernetes.created shouldBe empty
    registeredUids() shouldBe empty
  }

  it should "start and register a Jupyter for a user who has none" in {
    val kubernetes = new StubKubernetes
    val result = provisionerFor(kubernetes, (_, _) => true)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    kubernetes.created.map(_._1) shouldBe List(writerUid.intValue())
    registeredUids() shouldBe List(writerUid)
    result.map(_.internalUrl) shouldBe Some(
      s"http://${kubernetes.generatePodURI(writerUid)}${kubernetes.basePathFor(writerUid)}"
    )
  }

  it should "give the pod the user's own derived token" in {
    // What makes one user's token useless against another's Jupyter.
    val kubernetes = new StubKubernetes
    provisionerFor(kubernetes, (_, _) => true)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)
    kubernetes.created.map(_._2) shouldBe List(JupyterTokenDeriver.derive(writerUid, specSecret))
  }

  it should "reuse a registered Jupyter that still answers" in {
    registerJupyter(writerUid)
    val kubernetes = new StubKubernetes
    val result = provisionerFor(kubernetes, (_, _) => true)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    kubernetes.created shouldBe empty
    result.map(_.internalUrl) shouldBe Some("http://localhost:9100")
  }

  it should "rebuild a registered Jupyter whose pod is gone" in {
    // The row would otherwise outlive the pod and point every later request at nothing.
    registerJupyter(writerUid, internalUrl = "http://stale:8888")
    val kubernetes = new StubKubernetes
    val result = provisionerFor(kubernetes, (url, _) => url != "http://stale:8888")
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    kubernetes.deleted shouldBe List(writerUid.intValue())
    kubernetes.created.map(_._1) shouldBe List(writerUid.intValue())
    result.map(_.internalUrl) shouldBe Some(
      s"http://${kubernetes.generatePodURI(writerUid)}${kubernetes.basePathFor(writerUid)}"
    )
  }

  it should "register nothing and clean up when the pod never becomes ready" in {
    val kubernetes = new StubKubernetes
    val result = provisionerFor(kubernetes, (_, _) => false)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    result shouldBe None
    kubernetes.deleted shouldBe List(writerUid.intValue())
    registeredUids() shouldBe empty
  }

  it should "record the user's own base path in the internal address" in {
    // Jupyter serves /api under its base path too, so an address without the prefix would
    // make every later probe and contents call 404.
    val kubernetes = new StubKubernetes
    val result = provisionerFor(kubernetes, (_, _) => true)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    result.map(_.internalUrl) shouldBe Some(
      s"http://${kubernetes.generatePodURI(writerUid)}${kubernetes.basePathFor(writerUid)}"
    )
  }

  it should "build the public URL from the configured template" in {
    val kubernetes = new StubKubernetes
    val result =
      provisionerFor(kubernetes, (_, _) => true, "https://texera.example.com/jupyter/{uid}")
        .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)
    result.map(_.publicUrl) shouldBe Some(s"https://texera.example.com/jupyter/$writerUid")
  }

  it should "adopt an existing pod instead of creating a second one" in {
    // A pod can outlive its row, so provisioning must be idempotent on the Kubernetes side.
    val kubernetes = new StubKubernetes
    kubernetes.alreadyExists = true
    val result = provisionerFor(kubernetes, (_, _) => true)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    kubernetes.created shouldBe empty
    result should not be empty
    registeredUids() shouldBe List(writerUid)
  }

  it should "reuse one Kubernetes client across calls" in {
    // The client is built lazily and held, so a second request must not construct another.
    val kubernetes = new StubKubernetes
    val provisioner = provisionerFor(kubernetes, (_, _) => true)
    provisioner.ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)
    provisioner.ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    // Second call finds the row it just wrote, so it provisions once in total.
    kubernetes.created.map(_._1) shouldBe List(writerUid.intValue())
    registeredUids() shouldBe List(writerUid)
  }

  it should "refuse to provision once the concurrency cap is reached" in {
    // Every provision parks its request thread on the readiness wait, so the cap is what keeps
    // a burst of first-time users from draining the HTTP worker pool.
    val kubernetes = new StubKubernetes
    val result = provisionerFor(kubernetes, (_, _) => true, maxConcurrentProvisions = 0)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    result shouldBe empty
    kubernetes.created shouldBe empty
    registeredUids() shouldBe empty
  }

  it should "release its permit after provisioning succeeds" in {
    // A permit leak would wedge the service after maxConcurrentProvisions successful starts.
    // A second user, so this provisions twice rather than taking the fast path.
    val kubernetes = new StubKubernetes
    val provisioner = provisionerFor(kubernetes, (_, _) => true, maxConcurrentProvisions = 1)
    provisioner.ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)
    provisioner
      .ensure(readerUid, jupyterEnabled = true, tokenSecret = specSecret) should not be empty

    kubernetes.created.map(_._1) should contain allOf (writerUid.intValue(), readerUid.intValue())
  }

  it should "release its permit after provisioning fails" in {
    // The failure path returns through the same permit, so a cluster refusing pods must not
    // burn the cap down.
    val kubernetes = new StubKubernetes
    kubernetes.failCreate = true
    val provisioner = provisionerFor(kubernetes, (_, _) => true, maxConcurrentProvisions = 1)
    provisioner.ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret) shouldBe empty

    kubernetes.failCreate = false
    provisioner
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret) should not be empty
  }

  it should "not spend a permit on a user whose pod already works" in {
    // The fast path never blocks, so the cap must not apply to it.
    registerJupyter(writerUid, internalUrl = "http://live:8888")
    val kubernetes = new StubKubernetes
    val result = provisionerFor(kubernetes, (_, _) => true, maxConcurrentProvisions = 0)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    result.map(_.internalUrl) shouldBe Some("http://live:8888")
    kubernetes.created shouldBe empty
  }

  it should "rebuild a registered pod that no longer accepts its token" in {
    // After a token-secret rotation the pod is still alive but holds the superseded token,
    // so liveness alone would keep handing out a token the pod rejects.
    registerJupyter(writerUid, internalUrl = "http://rotated:8888")
    val kubernetes = new StubKubernetes
    val result = provisionerFor(kubernetes, (url, _) => url != "http://rotated:8888")
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    kubernetes.deleted shouldBe List(writerUid.intValue())
    kubernetes.created.map(_._1) shouldBe List(writerUid.intValue())
    result.map(_.internalUrl) shouldBe Some(
      s"http://${kubernetes.generatePodURI(writerUid)}${kubernetes.basePathFor(writerUid)}"
    )
  }

  it should "wait out a discarded pod before creating its replacement" in {
    // Deletion is asynchronous, and a Terminating pod still satisfies podExists. Returning
    // from discard too early would skip the create and poll the dying pod to the deadline.
    registerJupyter(writerUid, internalUrl = "http://rotated:8888")
    val kubernetes = new StubKubernetes
    kubernetes.terminatingPolls = 2
    val result = provisionerFor(kubernetes, (url, _) => url != "http://rotated:8888")
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    kubernetes.created.map(_._1) shouldBe List(writerUid.intValue())
    result.map(_.internalUrl) shouldBe Some(
      s"http://${kubernetes.generatePodURI(writerUid)}${kubernetes.basePathFor(writerUid)}"
    )
  }

  it should "not rebuild when the discarded pod never terminates" in {
    // A pod stuck Terminating keeps its name, so a create would be skipped. Reporting
    // unavailable beats polling a pod that cannot accept the new token.
    registerJupyter(writerUid, internalUrl = "http://rotated:8888")
    val kubernetes = new StubKubernetes
    kubernetes.terminatingPolls = Int.MaxValue
    val result = provisionerFor(kubernetes, (url, _) => url != "http://rotated:8888")
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    kubernetes.deleted shouldBe List(writerUid.intValue())
    kubernetes.created shouldBe empty
    result shouldBe empty
  }

  it should "pass the derived token to the readiness check, not just the address" in {
    // Guards the rotation fix: a probe that ignored the token would accept any live pod.
    val kubernetes = new StubKubernetes
    var seen: List[String] = Nil
    provisionerFor(kubernetes, (_, tok) => { seen ::= tok; true })
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    seen.distinct shouldBe List(JupyterTokenDeriver.derive(writerUid, specSecret))
  }

  it should "succeed when another request created the pod first" in {
    // Check-then-act on a uid-keyed pod: two concurrent first requests both find it absent
    // and both create. The loser's 409 is the winner's pod, which is the one it wanted.
    val kubernetes = new StubKubernetes
    kubernetes.createConflicts = true
    val result = provisionerFor(kubernetes, (_, _) => true)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    result.map(_.internalUrl) shouldBe Some(
      s"http://${kubernetes.generatePodURI(writerUid)}${kubernetes.basePathFor(writerUid)}"
    )
    kubernetes.deleted shouldBe empty
    registeredUids() shouldBe List(writerUid)
  }

  it should "report unavailable when the cluster refuses to create the pod" in {
    val kubernetes = new StubKubernetes
    kubernetes.failCreate = true
    val result = provisionerFor(kubernetes, (_, _) => true)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    result shouldBe None
    registeredUids() shouldBe empty
  }

  it should "still fail on a Kubernetes error that is not a conflict" in {
    // Guards the narrowness of the 409 catch: only AlreadyExists means someone else won.
    val kubernetes = new StubKubernetes {
      override def createPod(uid: Int, token: String) =
        throw new KubernetesClientException("quota exceeded", 403, null)
    }
    provisionerFor(kubernetes, (_, _) => true)
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret) shouldBe None
    registeredUids() shouldBe empty
  }

  it should "still rebuild when the stale pod cannot be deleted" in {
    // The pod may already be gone, which is the state the delete was trying to reach.
    registerJupyter(writerUid, internalUrl = "http://stale:8888")
    val kubernetes = new StubKubernetes
    kubernetes.failDelete = true
    val result = provisionerFor(kubernetes, (url, _) => url != "http://stale:8888")
      .ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    kubernetes.created.map(_._1) shouldBe List(writerUid.intValue())
    result.map(_.internalUrl) shouldBe Some(
      s"http://${kubernetes.generatePodURI(writerUid)}${kubernetes.basePathFor(writerUid)}"
    )
  }

  it should "report unavailable when registration fails for a reason other than a race" in {
    // A uid with no user row violates the foreign key. Only a duplicate primary key means
    // "another request won"; anything else has to surface rather than be swallowed.
    val orphanUid = 999999
    val kubernetes = new StubKubernetes
    val result = provisionerFor(kubernetes, (_, _) => true)
      .ensure(orphanUid, jupyterEnabled = true, tokenSecret = specSecret)

    result shouldBe None
    registeredUids() shouldBe empty
  }

  it should "keep the winning row when two requests provision at once" in {
    // The readiness probe runs just before the insert, so registering there stands in for a
    // concurrent request winning the race.
    val kubernetes = new StubKubernetes
    val racing = provisionerFor(
      kubernetes,
      (_, _) => { if (registeredUids().isEmpty) registerJupyter(writerUid); true }
    )
    val result = racing.ensure(writerUid, jupyterEnabled = true, tokenSecret = specSecret)

    result should not be empty
    registeredUids() shouldBe List(writerUid)
  }

  it should "fall back to the configured endpoints when none are passed" in {
    // The defaulted parameter is what keeps direct object calls working for callers that
    // have no per-user endpoints to hand in.
    withFakeJupyter(contentsStatus = 201) {
      val response = NotebookMigrationResource.getJupyterURL()
      response.getStatus shouldBe Response.Status.OK.getStatusCode
      response.getEntity.toString should include(JupyterEndpoints.configured.publicUrl)
    }
  }

  "the internal/public URL split" should "dial the internal URL and return only the public one" in {
    withFakeJupyter(contentsStatus = 201) {
      val urlResp = NotebookMigrationResource.getJupyterURL(splitEndpoints)
      urlResp.getStatus shouldBe Response.Status.OK.getStatusCode
      urlResp.getEntity.toString should include("192.0.2.1:1234")
      urlResp.getEntity.toString should not include "localhost:9100"

      val iframe = NotebookMigrationResource.getJupyterIframeURL("notebook.ipynb", splitEndpoints)
      iframe.getStatus shouldBe Response.Status.OK.getStatusCode
      iframe.getEntity.toString should include("192.0.2.1:1234")
      iframe.getEntity.toString should not include "localhost:9100"
    }
  }

  it should "send the notebook to the internal URL, not the public one" in {
    withFakeJupyter(contentsStatus = 201) {
      val resp = NotebookMigrationResource.setNotebook(
        """{"notebookName": "notebook.ipynb", "notebookData": {"cells": []}}""",
        splitEndpoints
      )
      // Reaching the stub at all proves the upload used internalUrl: the public one is
      // unroutable, so a swap would surface here as a 500.
      resp.getStatus shouldBe Response.Status.OK.getStatusCode
      lastContentsRequest shouldBe Some(("PUT", "/api/contents/work/notebook.ipynb"))
    }
  }

  it should "delete against the internal URL, not the public one" in {
    withFakeJupyter(contentsStatus = 204) {
      val resp = NotebookMigrationResource.deleteNotebook(
        """{"notebookName": "notebook.ipynb"}""",
        splitEndpoints
      )
      resp.getStatus shouldBe Response.Status.OK.getStatusCode
      lastContentsRequest shouldBe Some(("DELETE", "/api/contents/work/notebook.ipynb"))
    }
  }

  it should "report Jupyter unavailable when only the public URL is reachable" in {
    // The inverse of the tests above, and the one that catches the fields being swapped:
    // the reachability probe must follow internalUrl, so a reachable public URL must not
    // rescue an unreachable internal one.
    withFakeJupyter(contentsStatus = 201) {
      // Port 9 on loopback: refused immediately, so this fails fast and without DNS.
      val swapped = JupyterEndpoints(
        internalUrl = "http://127.0.0.1:9",
        publicUrl = "http://localhost:9100",
        token = "texera"
      )
      NotebookMigrationResource.getJupyterURL(swapped).getStatus shouldBe 500
      NotebookMigrationResource
        .getJupyterIframeURL("notebook.ipynb", swapped)
        .getStatus shouldBe 500
    }
  }

  it should "build the iframe URL from an explicit notebook name" in {
    withFakeJupyter(contentsStatus = 201) {
      val resp = resource.getJupyterIframeURL("other.ipynb", sessionUser(writerUid))
      resp.getStatus shouldBe Response.Status.OK.getStatusCode
      resp.getEntity.toString should include("/notebooks/work/other.ipynb")
    }
  }

  it should "reject an invalid notebook name for the iframe URL with 400" in {
    // notebookName flows into the URL, so it is validated before any Jupyter call and
    // rejected without a running server.
    NotebookMigrationResource
      .getJupyterIframeURL("../../etc/evil.ipynb")
      .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
  }

  it should "not be affected by a prior setNotebook call (no shared iframe state)" in {
    // Pins the stateless refactor: getJupyterIframeURL builds its URL from the request, not
    // from state left by setNotebook. A param-less iframe request after uploading other.ipynb
    // must return the default notebook, not the just-uploaded name.
    withFakeJupyter(contentsStatus = 201) {
      val user = sessionUser(writerUid)
      resource
        .setNotebook("""{"notebookName": "other.ipynb", "notebookData": {"cells": []}}""", user)
        .getStatus shouldBe Response.Status.OK.getStatusCode

      val iframe = resource.getJupyterIframeURL(null, user)
      iframe.getStatus shouldBe Response.Status.OK.getStatusCode
      iframe.getEntity.toString should include("/notebooks/work/notebook.ipynb")
      iframe.getEntity.toString should not include "other.ipynb"
    }
  }

  it should "treat a 403 from Jupyter's /api as reachable" in {
    // isJupyterAvailable accepts 200 OR 403 (403 = server up but auth-gated).
    withFakeJupyter(contentsStatus = 201, apiStatus = 403) {
      resource
        .getJupyterURL(sessionUser(writerUid))
        .getStatus shouldBe Response.Status.OK.getStatusCode
    }
  }

  it should "treat an unexpected /api status (neither 200 nor 403) as unavailable" in {
    withFakeJupyter(contentsStatus = 201, apiStatus = 500) {
      resource.getJupyterURL(sessionUser(writerUid)).getStatus shouldBe 500
    }
  }

  it should "treat a 200 from the contents API as a successful upload" in {
    // Jupyter returns 200 when overwriting an existing notebook, 201 when creating.
    withFakeJupyter(contentsStatus = 200) {
      val body = """{"notebookName": "notebook.ipynb", "notebookData": {"cells": []}}"""
      resource
        .setNotebook(body, sessionUser(writerUid))
        .getStatus shouldBe Response.Status.OK.getStatusCode
    }
  }

  // -- deleteNotebook (Jupyter file) ------------------------------------------

  "deleteNotebook" should "DELETE the notebook's contents path and report deleted=1" in {
    withFakeJupyter(contentsStatus = 204) {
      val name = s"notebook_$testWid.ipynb"
      val resp = resource.deleteNotebook(deleteNotebookPayload(name), sessionUser(writerUid))
      resp.getStatus shouldBe Response.Status.OK.getStatusCode
      resp.getEntity.toString should include("\"deleted\":1")
      // Pins the verb and the work/ path, the two things that make this the counterpart
      // of setNotebook's PUT rather than a delete of some other file.
      lastContentsRequest shouldBe Some(("DELETE", s"/api/contents/work/$name"))
    }
  }

  it should "treat a 200 from Jupyter as a successful delete, reporting deleted=1" in {
    // Some Jupyter versions answer 200 instead of 204 on a delete; both mean success.
    withFakeJupyter(contentsStatus = 200) {
      val resp = resource.deleteNotebook(deleteNotebookPayload(), sessionUser(writerUid))
      resp.getStatus shouldBe Response.Status.OK.getStatusCode
      resp.getEntity.toString should include("\"deleted\":1")
    }
  }

  it should "treat a 404 from Jupyter as a no-op, reporting deleted=0" in {
    // A workflow whose notebook was never uploaded must still delete cleanly.
    withFakeJupyter(contentsStatus = 404) {
      val resp = resource.deleteNotebook(deleteNotebookPayload(), sessionUser(writerUid))
      resp.getStatus shouldBe Response.Status.OK.getStatusCode
      resp.getEntity.toString should include("\"deleted\":0")
    }
  }

  it should "return 500 when Jupyter rejects the delete" in {
    withFakeJupyter(contentsStatus = 500) {
      resource
        .deleteNotebook(deleteNotebookPayload(), sessionUser(writerUid))
        .getStatus shouldBe 500
    }
  }

  it should "reject a notebook name that is not a plain .ipynb filename with 400" in {
    // Validated before any Jupyter call, so no server is needed. Covers path traversal,
    // a wrong extension, and an embedded subpath.
    Seq("../../etc/evil.ipynb", "notebook.txt", "work/notebook.ipynb").foreach { name =>
      withClue(s"name=$name: ") {
        NotebookMigrationResource
          .deleteNotebook(deleteNotebookPayload(name))
          .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
      }
      lastContentsRequest shouldBe None
    }
  }

  it should "return 400 when 'notebookName' is missing or not a string" in {
    // A missing name must be a client error, not a 500 from null.asText().
    Seq("""{}""", """{"notebookName": 7}""").foreach { body =>
      withClue(s"body=$body: ") {
        NotebookMigrationResource
          .deleteNotebook(body)
          .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
      }
    }
  }

  it should "return 400 when the request body is malformed JSON" in {
    resource
      .deleteNotebook("not json", sessionUser(writerUid))
      .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
  }

  // -- setNotebook ------------------------------------------------------------

  "setNotebook" should "reject a notebook name that is not a plain .ipynb filename with 400" in {
    // The name is validated before any Jupyter call, so these are rejected with a
    // 400 without a running Jupyter server. Covers path traversal, a wrong
    // extension, and an embedded subpath.
    Seq("../../etc/evil.ipynb", "notebook.txt", "work/notebook.ipynb").foreach { name =>
      val body = s"""{"notebookName": "$name", "notebookData": {"cells": []}}"""
      withClue(s"name=$name: ") {
        NotebookMigrationResource
          .setNotebook(body)
          .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
      }
    }
  }
}
