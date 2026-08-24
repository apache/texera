// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera.service.resource

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs._
import jakarta.ws.rs.core._
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.jooq.JSONB
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.apache.texera.dao.jooq.generated.tables.Notebook
import org.apache.texera.dao.jooq.generated.tables.WorkflowNotebookMapping
import org.apache.texera.dao.jooq.generated.tables.WorkflowVersion
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal
import org.apache.texera.common.config.StorageConfig

object NotebookMigrationResource extends LazyLogging {

  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  // Build an error response body via the mapper so the message is JSON-escaped; interpolating
  // e.getMessage directly produces malformed JSON when it contains quotes, backslashes, or newlines.
  private def errorJson(message: String): String =
    mapper.writeValueAsString(mapper.createObjectNode().put("error", message))

  // Build a {"success": true, "url": ...} body via the mapper so the URL is JSON-escaped
  // rather than raw-interpolated.
  private def successUrlJson(url: String): String =
    mapper.writeValueAsString(mapper.createObjectNode().put("success", true).put("url", url))

  // Build a {"success": true, "deleted": <count>} body via the mapper. The count lets the
  // caller distinguish a real deletion (1) from a no-op when nothing was stored (0).
  private def successDeletedJson(deleted: Int): String =
    mapper.writeValueAsString(
      mapper.createObjectNode().put("success", true).put("deleted", deleted)
    )

  private def jupyterUnavailableResponse: Response =
    Response
      .status(500)
      .entity(
        mapper.writeValueAsString(
          mapper
            .createObjectNode()
            .put("success", false)
            .put("message", "Cannot connect to Jupyter server")
        )
      )
      .build()

  // Parse a request body into a JSON object. Returns Left(400) on malformed JSON or a
  // non-object root so a bad request is reported as a client error.
  private def parseBody(body: String): Either[Response, JsonNode] = {
    val json =
      try mapper.readTree(body)
      catch { case NonFatal(_) => null }
    if (json == null || !json.isObject) {
      Left(
        Response
          .status(Response.Status.BAD_REQUEST)
          .entity(errorJson("Request body must be a JSON object"))
          .build()
      )
    } else {
      Right(json)
    }
  }

  // Read the required integer `wid` from a request body. Returns Left(400) when the field is
  // missing or not an integer so the caller can short-circuit. Without this a missing wid NPEs
  // into a 500 and a non-integer wid silently coerces to 0 via asInt().
  private def readWid(json: JsonNode): Either[Response, java.lang.Integer] = {
    val widNode = json.get("wid")
    if (widNode == null || !widNode.isInt) {
      Left(
        Response
          .status(Response.Status.BAD_REQUEST)
          .entity(errorJson("Missing or invalid 'wid'"))
          .build()
      )
    } else {
      Right(widNode.asInt())
    }
  }

  // jupyterUrl and jupyterToken are single process-wide values, so this service still
  // targets one Jupyter per process (the per-user-pod model) and must not be deployed as a
  // shared global instance yet: every user would get the same Jupyter and the same token.
  // Resolving these per user is a later stage of the migration (#7665).
  private val jupyterUrl = StorageConfig.jupyterURL
  private val jupyterToken = StorageConfig.jupyterToken

  // Default notebook name used when a request does not specify one, so a param-less
  // getJupyterIframeURL call reproduces the URL from before this service became stateless.
  private val defaultNotebookName = "notebook.ipynb"

  private def isJupyterAvailable(jupyterUrl: String): Boolean = {
    var conn: java.net.HttpURLConnection = null
    try {
      conn = new java.net.URL(s"$jupyterUrl/api")
        .openConnection()
        .asInstanceOf[java.net.HttpURLConnection]

      conn.setRequestMethod("GET")
      conn.setConnectTimeout(2000)
      conn.setReadTimeout(2000)

      val status = conn.getResponseCode

      status == 200 || status == 403
    } catch {
      case _: Exception => false
    } finally {
      if (conn != null) conn.disconnect()
    }
  }

  // Returns the Jupyter iframe reference URL for the given notebook.
  def getJupyterIframeURL(notebookName: String): Response = {
    // notebookName flows into the returned URL, so validate it the same way setNotebook does:
    // block path traversal and keep it to a plain .ipynb filename.
    if (!notebookName.matches("[A-Za-z0-9._-]+\\.ipynb")) {
      return Response
        .status(Response.Status.BAD_REQUEST)
        .entity(errorJson(s"Invalid notebook name: $notebookName"))
        .build()
    }

    if (!isJupyterAvailable(jupyterUrl)) {
      return jupyterUnavailableResponse
    }

    Response
      .ok(successUrlJson(s"$jupyterUrl/notebooks/work/$notebookName?token=$jupyterToken"))
      .build()
  }

  // Returns the URL of Jupyter
  def getJupyterURL(): Response = {
    if (!isJupyterAvailable(jupyterUrl)) {
      return jupyterUnavailableResponse
    }

    Response.ok(successUrlJson(jupyterUrl)).build()
  }

  // Set the notebook in Jupyter
  def setNotebook(body: String): Response = {
    var conn: HttpURLConnection = null
    try {
      val json = parseBody(body) match {
        case Left(badRequest) => return badRequest
        case Right(j)         => j
      }

      val notebookName = json.get("notebookName").asText()
      val notebookData = json.get("notebookData")

      // Allow only a plain ".ipynb" filename. Validated before any network call so a
      // bad name is rejected with a 400 up front. This blocks path traversal in the
      // Jupyter contents URL (e.g. "../../etc/x.ipynb").
      if (!notebookName.matches("[A-Za-z0-9._-]+\\.ipynb")) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(errorJson(s"Invalid notebook name: $notebookName"))
          .build()
      }

      if (!isJupyterAvailable(jupyterUrl)) {
        return jupyterUnavailableResponse
      }

      // Construct Jupyter API URL
      val apiUrl = s"$jupyterUrl/api/contents/work/$notebookName"

      val url = new URL(apiUrl)
      conn = url.openConnection().asInstanceOf[HttpURLConnection]

      conn.setRequestMethod("PUT")
      conn.setDoOutput(true)
      conn.setRequestProperty("Content-Type", "application/json")
      // The Jupyter Contents API requires authentication; send the configured token.
      conn.setRequestProperty("Authorization", s"token $jupyterToken")

      val requestBody =
        s"""
      {
        "type": "notebook",
        "content": $notebookData
      }
      """

      val os = conn.getOutputStream
      os.write(requestBody.getBytes(StandardCharsets.UTF_8))
      os.flush()
      os.close()

      val status = conn.getResponseCode

      if (status != 200 && status != 201) {
        return Response
          .status(500)
          .entity(
            s"""
        {
          "success": false,
          "message": "Failed to upload notebook to Jupyter (status $status)"
        }
        """
          )
          .build()
      }

      Response
        .ok(
          s"""
      {
        "success": true,
        "message": "Notebook successfully sent to Jupyter."
      }
      """
        )
        .build()

    } catch {
      case NonFatal(e) =>
        logger.error("Error sending notebook to Jupyter", e)
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(errorJson(e.getMessage))
          .build()
    } finally {
      if (conn != null) conn.disconnect()
    }
  }

  // Delete the notebook file from Jupyter's work/ directory:
  def deleteNotebook(body: String): Response = {
    var conn: HttpURLConnection = null
    try {
      val json = parseBody(body) match {
        case Left(badRequest) => return badRequest
        case Right(j)         => j
      }

      // Read the name defensively
      val notebookName =
        Option(json.get("notebookName")).filter(_.isTextual).map(_.asText()).getOrElse("")

      if (!notebookName.matches("[A-Za-z0-9._-]+\\.ipynb")) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(errorJson(s"Invalid notebook name: $notebookName"))
          .build()
      }

      if (!isJupyterAvailable(jupyterUrl)) {
        return jupyterUnavailableResponse
      }

      val url = new URL(s"$jupyterUrl/api/contents/work/$notebookName")
      conn = url.openConnection().asInstanceOf[HttpURLConnection]

      conn.setRequestMethod("DELETE")
      conn.setConnectTimeout(2000)
      conn.setReadTimeout(2000)
      conn.setRequestProperty("Authorization", s"token $jupyterToken")

      val status = conn.getResponseCode

      // Jupyter answers 204 on a successful delete, or 200 when it echoes the deleted entry.
      // A 404 means the file is already gone, which is the requested end state, so report it
      // as a no-op (deleted=0) rather than an error: a workflow whose notebook was never
      // uploaded must still delete cleanly.
      if (status != 204 && status != 200 && status != 404) {
        return Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(errorJson(s"Failed to delete notebook from Jupyter (status $status)"))
          .build()
      }

      Response.ok(successDeletedJson(if (status == 404) 0 else 1)).build()

    } catch {
      case NonFatal(e) =>
        logger.error("Error deleting notebook from Jupyter", e)
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(errorJson(e.getMessage))
          .build()
    } finally {
      if (conn != null) conn.disconnect()
    }
  }

  // Store notebook + mapping in database
  def storeNotebookAndMapping(body: String, uid: java.lang.Integer): Response = {
    try {
      val json = parseBody(body) match {
        case Left(badRequest) => return badRequest
        case Right(j)         => j
      }

      val wid: java.lang.Integer = readWid(json) match {
        case Left(badRequest) => return badRequest
        case Right(w)         => w
      }
      val mappingNode = json.get("mapping")
      val notebookNode = json.get("notebook")

      // Only a user with write access to the workflow may store its notebook.
      if (!WorkflowAccessResource.hasWriteAccess(wid, uid)) {
        return Response
          .status(Response.Status.FORBIDDEN)
          .entity(errorJson(s"No write access to workflow $wid"))
          .build()
      }

      val dsl = SqlServer.getInstance().createDSLContext()

      // notebook.wid is UNIQUE: a workflow has at most one notebook. If one already
      // exists, reject the re-store with a 409 rather than letting the INSERT trip the
      // constraint and surface as a 500. Checked before the version lookup so a re-store
      // skips that query.
      val alreadyStored = dsl.fetchExists(
        dsl.selectFrom(Notebook.NOTEBOOK).where(Notebook.NOTEBOOK.WID.eq(wid))
      )
      if (alreadyStored) {
        return Response
          .status(Response.Status.CONFLICT)
          .entity(errorJson(s"A notebook is already stored for workflow $wid"))
          .build()
      }

      // The mapping's vid FK must reference a real workflow_version row. Anchor it to the
      // workflow's own latest version (created alongside the workflow) rather than a
      // hardcoded id, so an unrelated workflow's version can never own or cascade it.
      val vid: java.lang.Integer = dsl
        .select(DSL.max(WorkflowVersion.WORKFLOW_VERSION.VID))
        .from(WorkflowVersion.WORKFLOW_VERSION)
        .where(WorkflowVersion.WORKFLOW_VERSION.WID.eq(wid))
        .fetchOne(0, classOf[java.lang.Integer])
      if (vid == null) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(errorJson(s"No workflow version exists for workflow $wid"))
          .build()
      }

      val nid: java.lang.Integer = SqlServer.withTransaction(dsl) { ctx =>
        // Insert notebook
        val notebookRecord = ctx
          .insertInto(Notebook.NOTEBOOK)
          .set(Notebook.NOTEBOOK.WID, wid)
          .set(Notebook.NOTEBOOK.NOTEBOOK_, JSONB.valueOf(notebookNode.toString))
          .returning(Notebook.NOTEBOOK.NID)
          .fetchOne()

        val nidInside: java.lang.Integer = notebookRecord.getValue(Notebook.NOTEBOOK.NID)

        // Insert workflow-notebook mapping
        ctx
          .insertInto(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING)
          .set(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.WID, wid)
          .set(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.VID, vid)
          .set(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.NID, nidInside)
          .set(
            WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.MAPPING,
            JSONB.valueOf(mappingNode.toString)
          )
          .execute()

        nidInside
      }

      Response
        .ok(
          s"""
      {
        "success": true,
        "message": "Notebook and mapping successfully stored. wid: $wid, vid: $vid, nid: $nid"
      }
      """
        )
        .build()

    } catch {
      // Backstop for the pre-check TOCTOU race: two writers on a shared workflow can both
      // pass the existence check, then one INSERT trips the UNIQUE(wid) constraint. Translate
      // that (Postgres SQLState 23505) to a 409 rather than a generic 500.
      case e: DataAccessException if e.sqlState == "23505" =>
        Response
          .status(Response.Status.CONFLICT)
          .entity(errorJson("A notebook is already stored for this workflow"))
          .build()
      case NonFatal(e) =>
        logger.error("Error storing mapping and workflow", e)
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(errorJson(e.getMessage))
          .build()
    }
  }

  // Fetch notebook + mapping
  def fetchNotebookAndMapping(body: String, uid: java.lang.Integer): Response = {
    try {
      val json = parseBody(body) match {
        case Left(badRequest) => return badRequest
        case Right(j)         => j
      }

      val wid: java.lang.Integer = readWid(json) match {
        case Left(badRequest) => return badRequest
        case Right(w)         => w
      }
      // Only a user with write access to the workflow may fetch its notebook.
      if (!WorkflowAccessResource.hasWriteAccess(wid, uid)) {
        return Response
          .status(Response.Status.FORBIDDEN)
          .entity(errorJson(s"No write access to workflow $wid"))
          .build()
      }

      val dsl = SqlServer.getInstance().createDSLContext()

      // Fetch the notebook for this workflow, regardless of its version.
      //
      // Future work: to support one notebook per workflow version, drop the notebook.wid
      // UNIQUE constraint and add a vid filter here.
      val result = dsl
        .select(
          Notebook.NOTEBOOK.NID,
          Notebook.NOTEBOOK.NOTEBOOK_,
          WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.MAPPING
        )
        .from(Notebook.NOTEBOOK)
        .join(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING)
        .on(Notebook.NOTEBOOK.WID.eq(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.WID))
        .and(Notebook.NOTEBOOK.NID.eq(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.NID))
        .where(Notebook.NOTEBOOK.WID.eq(wid))
        .orderBy(Notebook.NOTEBOOK.NID.desc()) // most recent nid first
        .limit(1) // only take the latest
        .fetchOne()

      if (result == null) {
        Response.ok("""{"exists": false}""").build()
      } else {
        val nid: Int = result.getValue(Notebook.NOTEBOOK.NID)
        val notebookJson: String =
          result.get(Notebook.NOTEBOOK.NOTEBOOK_).asInstanceOf[JSONB].data()
        val mappingJson: String = result
          .get(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.MAPPING)
          .asInstanceOf[JSONB]
          .data()

        Response
          .ok(
            s"""
        {
          "exists": true,
          "notebook": $notebookJson,
          "mapping": $mappingJson
        }
        """
          )
          .build()
      }

    } catch {
      case NonFatal(e) =>
        logger.error("Database error retrieving mapping", e)
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(errorJson(e.getMessage))
          .build()
    }
  }

  // Delete notebook + mapping for a workflow. The notebook -> workflow_notebook_mapping FK is
  // ON DELETE CASCADE, so deleting the notebook row removes its mapping rows too. notebook.wid
  // is UNIQUE (one notebook per workflow), so wid alone identifies the row and vid is not needed.
  def deleteNotebookAndMapping(body: String, uid: java.lang.Integer): Response = {
    try {
      val json = parseBody(body) match {
        case Left(badRequest) => return badRequest
        case Right(j)         => j
      }

      val wid: java.lang.Integer = readWid(json) match {
        case Left(badRequest) => return badRequest
        case Right(w)         => w
      }

      // Only a user with write access to the workflow may delete its notebook.
      if (!WorkflowAccessResource.hasWriteAccess(wid, uid)) {
        return Response
          .status(Response.Status.FORBIDDEN)
          .entity(errorJson(s"No write access to workflow $wid"))
          .build()
      }

      val dsl = SqlServer.getInstance().createDSLContext()

      // execute() returns the affected row count: 1 when a notebook was removed, 0 when the
      // workflow had nothing stored (idempotent no-op).
      val deleted: Int = SqlServer.withTransaction(dsl) { ctx =>
        ctx
          .deleteFrom(Notebook.NOTEBOOK)
          .where(Notebook.NOTEBOOK.WID.eq(wid))
          .execute()
      }

      Response.ok(successDeletedJson(deleted)).build()

    } catch {
      case NonFatal(e) =>
        logger.error("Error deleting notebook and mapping", e)
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(errorJson(e.getMessage))
          .build()
    }
  }
}

@Path("/notebook-migration")
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class NotebookMigrationResource extends LazyLogging {

  @GET
  @Path("/get-jupyter-iframe-url")
  def getJupyterIframeURL(
      @QueryParam("notebookName") notebookName: String,
      @Auth user: SessionUser
  ): Response = {
    logger.info("Getting Jupyter iframe URL")
    val name = Option(notebookName)
      .filter(_.nonEmpty)
      .getOrElse(NotebookMigrationResource.defaultNotebookName)
    NotebookMigrationResource.getJupyterIframeURL(name)
  }

  @GET
  @Path("/get-jupyter-url")
  def getJupyterURL(@Auth user: SessionUser): Response = {
    logger.info("Getting Jupyter API URL")
    NotebookMigrationResource.getJupyterURL()
  }

  @POST
  @Path("/set-notebook")
  def setNotebook(body: String, @Auth user: SessionUser): Response = {
    logger.info("Setting notebook")
    NotebookMigrationResource.setNotebook(body)
  }

  @POST
  @Path("/delete-notebook")
  def deleteNotebook(body: String, @Auth user: SessionUser): Response = {
    logger.info("Deleting notebook from Jupyter")
    NotebookMigrationResource.deleteNotebook(body)
  }

  @POST
  @Path("/store-notebook-and-mapping")
  def storeNotebookAndMapping(body: String, @Auth user: SessionUser): Response = {
    logger.info("Storing notebook and mapping")
    NotebookMigrationResource.storeNotebookAndMapping(body, user.getUid)
  }

  @POST
  @Path("/fetch-notebook-and-mapping")
  def fetchNotebookAndMapping(body: String, @Auth user: SessionUser): Response = {
    logger.info("Fetching notebook and mapping")
    NotebookMigrationResource.fetchNotebookAndMapping(body, user.getUid)
  }

  @POST
  @Path("/delete-notebook-and-mapping")
  def deleteNotebookAndMapping(body: String, @Auth user: SessionUser): Response = {
    logger.info("Deleting notebook and mapping")
    NotebookMigrationResource.deleteNotebookAndMapping(body, user.getUid)
  }
}
