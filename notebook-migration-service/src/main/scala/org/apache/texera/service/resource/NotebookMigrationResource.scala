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

import com.fasterxml.jackson.databind.ObjectMapper
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
import org.apache.texera.dao.jooq.generated.tables.Notebook
import org.apache.texera.dao.jooq.generated.tables.WorkflowNotebookMapping
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

  private val jupyterUrl = StorageConfig.jupyterURL
  private val jupyterToken = StorageConfig.jupyterToken
  // The token is passed as a URL param so the browser iframe can authenticate when loading the notebook.
  // jupyterIframeURL is process-global state. This is safe ONLY because each user runs their own pod
  // (own notebook-migration-service JVM + own Jupyter) in the k8s deployment, so this singleton is
  // effectively per-user. Do NOT deploy this service as a shared multi-user instance without adding
  // per-user keying here, or one user's upload would overwrite another's iframe URL.
  @volatile private var jupyterIframeURL =
    s"$jupyterUrl/notebooks/work/notebook.ipynb?token=$jupyterToken"

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

  // Returns the Jupyter iframe reference URL
  def getJupyterIframeURL(): Response = {
    if (!isJupyterAvailable(jupyterUrl)) {
      return Response
        .status(500)
        .entity(
          """
      {
        "success": false,
        "message": "Cannot connect to Jupyter server"
      }
      """
        )
        .build()
    }

    Response.ok(successUrlJson(jupyterIframeURL)).build()
  }

  // Returns the URL of Jupyter
  def getJupyterURL(): Response = {
    if (!isJupyterAvailable(jupyterUrl)) {
      return Response
        .status(500)
        .entity(
          """
      {
        "success": false,
        "message": "Cannot connect to Jupyter server"
      }
      """
        )
        .build()
    }

    Response.ok(successUrlJson(jupyterUrl)).build()
  }

  // Set the notebook in Jupyter
  def setNotebook(body: String): Response = {
    var conn: HttpURLConnection = null
    try {
      val json = mapper.readTree(body)

      val notebookName = json.get("notebookName").asText()
      val notebookData = json.get("notebookData")

      // Allow only a plain ".ipynb" filename. Validated before any network call so a
      // bad name is rejected with a 400 up front. This blocks path traversal in the
      // Jupyter contents URL (e.g. "../../etc/x.ipynb") and keeps notebookName out of
      // the raw-interpolated jupyterIframeURL JSON (no quotes/control chars).
      if (!notebookName.matches("[A-Za-z0-9._-]+\\.ipynb")) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(errorJson(s"Invalid notebook name: $notebookName"))
          .build()
      }

      if (!isJupyterAvailable(jupyterUrl)) {
        return Response
          .status(500)
          .entity(
            """
        {
          "success": false,
          "message": "Cannot connect to Jupyter server"
        }
        """
          )
          .build()
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

      jupyterIframeURL = s"$jupyterUrl/notebooks/work/$notebookName?token=$jupyterToken"

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

  // Store notebook + mapping in database
  def storeNotebookAndMapping(body: String, uid: java.lang.Integer): Response = {
    try {
      val json = mapper.readTree(body)

      val wid: java.lang.Integer = json.get("wid").asInt()
      val vid: java.lang.Integer = json.get("vid").asInt()
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
      // constraint and surface as a 500.
      val alreadyStored = dsl.fetchExists(
        dsl.selectFrom(Notebook.NOTEBOOK).where(Notebook.NOTEBOOK.WID.eq(wid))
      )
      if (alreadyStored) {
        return Response
          .status(Response.Status.CONFLICT)
          .entity(errorJson(s"A notebook is already stored for workflow $wid"))
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
      val json = mapper.readTree(body)

      val wid: java.lang.Integer = json.get("wid").asInt()
      val vid: java.lang.Integer = json.get("vid").asInt()

      // Only a user with write access to the workflow may fetch its notebook.
      if (!WorkflowAccessResource.hasWriteAccess(wid, uid)) {
        return Response
          .status(Response.Status.FORBIDDEN)
          .entity(errorJson(s"No write access to workflow $wid"))
          .build()
      }

      val dsl = SqlServer.getInstance().createDSLContext()

      // Fetch the most recent notebook (highest nid) for this workflow version
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
        .and(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.VID.eq(vid))
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
}

@Path("/notebook-migration")
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class NotebookMigrationResource extends LazyLogging {

  @GET
  @Path("/get-jupyter-iframe-url")
  def getJupyterIframeURL(@Auth user: SessionUser): Response = {
    logger.info("Getting Jupyter iframe URL")
    NotebookMigrationResource.getJupyterIframeURL()
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
}
