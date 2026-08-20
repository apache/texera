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
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.Notebook.NOTEBOOK
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.Workflow.WORKFLOW
import org.apache.texera.dao.jooq.generated.tables.WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING
import org.apache.texera.dao.jooq.generated.tables.WorkflowUserAccess.WORKFLOW_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.WorkflowVersion.WORKFLOW_VERSION
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowUserAccessDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
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
