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

import com.fasterxml.jackson.databind.ObjectMapper
import org.glassfish.jersey.server.ChunkedOutput

import java.util.concurrent.LinkedBlockingQueue
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import java.util

case class PackageResponse(system: java.util.List[String], user: java.util.List[String])

@Path("/pve")
@Consumes(Array(MediaType.APPLICATION_JSON))
class PveResource {

  // --------------------------------------------------
  // Create / Install packages (SSE)
  // --------------------------------------------------
  @GET
  @Produces(Array("text/event-stream"))
  def createPve(
      @QueryParam("packages") packagesJson: String,
      @QueryParam("cuid") cuid: Int,
      @QueryParam("pveName") pveName: String
  ): ChunkedOutput[String] = {

    if (cuid == 0) {
      throw new BadRequestException("Missing or invalid cuid")
    }

    if (pveName == null || pveName.trim.isEmpty) {
      throw new BadRequestException("Missing environment name")
    }

    val mapper = new ObjectMapper()
    val packages =
      if (packagesJson == null || packagesJson.trim.isEmpty)
        List.empty[String]
      else
        mapper.readValue(packagesJson, classOf[Array[String]]).toList

    val queue = new LinkedBlockingQueue[String]()
    val chunkedOutput = new ChunkedOutput[String](classOf[String])

    Future {
      try {

        if (!PveManager.pveExists(cuid, pveName)) {
          PveManager.createNewPve(cuid, queue, pveName)
        }

        if (packages.nonEmpty) {
          PveManager.installPackages(packages, cuid, queue, pveName)
        }

      } catch {
        case e: Exception =>
          queue.put(s"[ERR] ${e.getMessage}")
      } finally {
        queue.put("__DONE__")
      }
    }

    Future {
      var done = false
      while (!done) {
        val line = queue.take()
        if (line == "__DONE__") {
          chunkedOutput.write("data: __DONE__\n\n")
          done = true
        } else chunkedOutput.write(s"data: $line\n\n")
      }
      chunkedOutput.close()
    }

    chunkedOutput
  }

  // --------------------------------------------------
  // Get installed packages
  // --------------------------------------------------
  @GET
  @Path("/packages")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getInstalledPackages(
      @QueryParam("cuid") cuid: Int,
      @QueryParam("pveName") pveName: String
  ): util.Map[String, util.List[String]] = {

    if (cuid == 0) {
      throw new BadRequestException("Missing or invalid cuid")
    }

    if (pveName == null || pveName.trim.isEmpty) {
      throw new BadRequestException("Missing environment name")
    }

    try {

      println(s"[PVE] HIT getInstalledPackages cuid=$cuid")

      val (systemPkgsRaw, userPkgsRaw) = PveManager.getSystemAndUserPackages(cuid, pveName)

      println(s"[PVE] raw systemPkgsRaw isNull=${systemPkgsRaw == null} value=$systemPkgsRaw")
      println(s"[PVE] raw userPkgsRaw   isNull=${userPkgsRaw == null} value=$userPkgsRaw")

      val systemPkgs = Option(systemPkgsRaw).getOrElse(Seq.empty[String]).toList.asJava
      val userPkgs = Option(userPkgsRaw).getOrElse(Seq.empty[String]).toList.asJava

      val resp = Map("system" -> systemPkgs, "user" -> userPkgs).asJava

      println(
        s"[PVE] returning keys=${resp.keySet()} systemSize=${systemPkgs.size()} userSize=${userPkgs.size()}"
      )

      resp

    } catch {
      case e: Exception =>
        e.printStackTrace()
        Map(
          "system" -> List(s"Error: ${e.getMessage}").asJava,
          "user" -> List.empty[String].asJava
        ).asJava
    }
  }

  // --------------------------------------------------
  // Fetch PVEs
  // --------------------------------------------------
  @GET
  @Path("/pves")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def fetchPVEs(@QueryParam("cuid") cuid: Int): util.List[util.Map[String, Object]] = {

    if (cuid == 0) {
      throw new BadRequestException("Missing or invalid cuid")
    }

    try {
      println(s"[PVE] HIT getInstalledPackages cuid=$cuid")

      val result = PveManager.getAllPveUserPackages(cuid)

      val resp = result
        .map {
          case (pveName, userPkgs) =>
            Map(
              "pveName" -> pveName,
              "userPackages" -> userPkgs.toList.asJava
            ).asJava
        }
        .toList
        .asJava

      println(s"[PVE] returning ${resp.size()} environments")
      resp

    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new InternalServerErrorException(s"Failed to get packages: ${e.getMessage}")
    }
  }

  // --------------------------------------------------
  // Uninstall package
  // --------------------------------------------------
  @POST
  @Path("/uninstall/{packageName}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def uninstallPackage(
      @PathParam("packageName") packageName: String,
      @QueryParam("cuid") cuid: Int,
      @QueryParam("pveName") pveName: String
  ): java.util.List[String] = {

    if (cuid == 0) {
      throw new BadRequestException("Missing or invalid cuid")
    }

    if (pveName == null || pveName.trim.isEmpty) {
      throw new BadRequestException("Missing environment name")
    }

    try {
      val output = PveManager.deletePackages(cuid, packageName, pveName)
      output.asJava
    } catch {
      case e: Exception =>
        e.printStackTrace()
        List(
          s"Error uninstalling package '$packageName': ${e.getMessage}"
        ).asJava
    }
  }

  // --------------------------------------------------
  // Get list of existing environments
  // --------------------------------------------------
  @GET
  @Path("/environments")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getEnvironments(@QueryParam("cuid") cuid: Int): java.util.List[String] = {
    PveManager.getEnvironments(cuid).asJava
  }
}
