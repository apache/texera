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

package org.apache.texera.web.resource.pythonvirtualenvironment

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Auth
import org.apache.texera.auth.SessionUser
import org.apache.texera.config.KubernetesConfig
import org.jooq.exception.DataAccessException

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters._
import java.util
import javax.ws.rs.DELETE
import javax.ws.rs.PathParam
import javax.ws.rs.core.Response

object PveResource {
  case class SavePvePayload(name: String, packages: Map[String, String])
  case class PveListItem(veid: Int, name: String, packages: Map[String, String])

  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val packagesType = new TypeReference[java.util.Map[String, String]] {}
}

@Path("/pve")
@Consumes(Array(MediaType.APPLICATION_JSON))
class PveResource extends LazyLogging {
  import PveResource._
  // --------------------------------------------------
  // Get system packages
  // --------------------------------------------------
  @GET
  @Path("/system")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getSystemPackages: util.Map[String, util.List[String]] = {
    val isLocal = !KubernetesConfig.kubernetesComputingUnitEnabled
    try {
      val systemPkgs =
        PveManager.getSystemPackages(isLocal).toList.asJava

      Map("system" -> systemPkgs).asJava
    } catch {
      case e: Exception =>
        logger.error("Failed to get system packages", e)
        throw new InternalServerErrorException(
          "Failed to get system packages."
        )
    }
  }

  // --------------------------------------------------
  // List all PVEs for the current user from the database
  // --------------------------------------------------
  @GET
  @Path("/db")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def listPves(@Auth sessionUser: SessionUser): java.util.List[PveListItem] = {
    PveManager
      .listPvesForUser(sessionUser.getUid.intValue())
      .map { stored =>
        val packages: Map[String, String] =
          try mapper.readValue(stored.packagesJson, packagesType).asScala.toMap
          catch { case _: Throwable => Map.empty[String, String] }
        PveListItem(stored.veid, stored.name, packages)
      }
      .asJava
  }

  // --------------------------------------------------
  // Update a PVE row owned by the current user
  // --------------------------------------------------
  @PUT
  @Path("/db/{veid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def updatePve(
      @PathParam("veid") veid: Int,
      payload: SavePvePayload,
      @Auth sessionUser: SessionUser
  ): Response = {
    val name = Option(payload.name).map(_.trim).getOrElse("")
    if (!PveManager.isValidPveName(name)) {
      return Response.status(Response.Status.BAD_REQUEST).entity("invalid name").build()
    }
    try {
      val packagesJson = mapper.writeValueAsString(payload.packages)
      val updated = PveManager.updatePve(veid, sessionUser.getUid.intValue(), name, packagesJson)
      if (updated) Response.ok(Map("veid" -> veid).asJava).build()
      else Response.status(Response.Status.NOT_FOUND).build()
    } catch {
      case e: DataAccessException if e.sqlState() == "23505" =>
        Response
          .status(Response.Status.CONFLICT)
          .entity(s"""An environment named "$name" already exists.""")
          .build()
      case e: Exception =>
        logger.error("Failed to update PVE", e)
        throw new InternalServerErrorException(s"Failed to update PVE: ${e.getMessage}")
    }
  }

  // --------------------------------------------------
  // Delete a PVE row owned by the current user
  // --------------------------------------------------
  @DELETE
  @Path("/db/{veid}")
  def deletePveFromDb(@PathParam("veid") veid: Int, @Auth sessionUser: SessionUser): Response = {
    val deleted = PveManager.deletePveFromDb(veid, sessionUser.getUid.intValue())
    if (deleted) Response.noContent().build()
    else Response.status(Response.Status.NOT_FOUND).build()
  }

  // --------------------------------------------------
  // Save a PVE (name + packages) to the database for the current user
  // --------------------------------------------------
  @POST
  @Path("/db")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def savePve(payload: SavePvePayload, @Auth sessionUser: SessionUser): Response = {
    val name = Option(payload.name).map(_.trim).getOrElse("")
    if (!PveManager.isValidPveName(name)) {
      return Response.status(Response.Status.BAD_REQUEST).entity("invalid name").build()
    }
    try {
      val packagesJson = mapper.writeValueAsString(payload.packages)
      val veid = PveManager.savePve(sessionUser.getUid.intValue(), name, packagesJson)
      Response.status(Response.Status.CREATED).entity(Map("veid" -> veid).asJava).build()
    } catch {
      case e: DataAccessException if e.sqlState() == "23505" =>
        Response
          .status(Response.Status.CONFLICT)
          .entity(s"""An environment named "$name" already exists.""")
          .build()
      case e: Exception =>
        logger.error("Failed to save PVE", e)
        throw new InternalServerErrorException(s"Failed to save PVE: ${e.getMessage}")
    }
  }

  // --------------------------------------------------
  // Fetch PVEs and Installed User Packages
  // --------------------------------------------------
  @GET
  @Path("/pves")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def fetchPVEs(@QueryParam("cuid") cuid: java.lang.Integer): Response = {
    if (cuid == null) {
      return Response
        .status(Response.Status.BAD_REQUEST) // safeguard against cuid = 0
        .entity("cuid query parameter is required")
        .build()
    }
    try {
      val pves = PveManager
        .getEnvironments(cuid)
        .map { pve =>
          Map(
            "pveName" -> pve.pveName.asInstanceOf[Object],
            "userPackages" -> pve.userPackages.asJava.asInstanceOf[Object]
          ).asJava
        }
        .asJava
      Response.ok(pves).build()
    } catch {
      case e: Exception =>
        logger.error("Failed to get PVEs", e)
        throw new InternalServerErrorException(s"Failed to get PVEs: ${e.getMessage}")
    }
  }

  // --------------------------------------------------
  // Delete PVEs
  // --------------------------------------------------
  @DELETE
  @Path("/pves/{cuId}")
  def deleteEnvironments(@PathParam("cuId") cuid: Int): Unit = {
    PveManager.deleteEnvironments(cuid)
  }

  // --------------------------------------------------
  // Delete User Installed Package
  // --------------------------------------------------
  @DELETE
  @Path("/{cuid}/{pveName}/packages/{packageName}")
  def deletePackage(
      @PathParam("cuid") cuid: Int,
      @PathParam("pveName") pveName: String,
      @PathParam("packageName") packageName: String
  ): Response = {
    val isLocal = !KubernetesConfig.kubernetesComputingUnitEnabled
    val messages = PveManager.deletePackages(
      cuid,
      packageName,
      pveName,
      isLocal
    )

    if (messages.exists(_.contains("[PVE][ERR]"))) {
      Response.status(Response.Status.BAD_REQUEST).entity(messages.asJava).build()
    } else {
      Response.ok(messages.asJava).build()
    }
  }

}
