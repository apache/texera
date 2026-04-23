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
    val queue = new LinkedBlockingQueue[String]()
    val chunkedOutput = new ChunkedOutput[String](classOf[String])

    Future {
      try {

        if (!PveManager.pveExists(cuid, pveName)) {
          PveManager.createNewPve(cuid, queue, pveName)
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
    try {
      val (systemPkgsRaw, userPkgsRaw) = PveManager.getSystemAndUserPackages(cuid, pveName)

      val systemPkgs = Option(systemPkgsRaw).getOrElse(Seq.empty[String]).toList.asJava
      val userPkgs = Option(userPkgsRaw).getOrElse(Seq.empty[String]).toList.asJava

      Map("system" -> systemPkgs, "user" -> userPkgs).asJava
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new InternalServerErrorException("Failed to get installed packages.")
    }
  }

  // --------------------------------------------------
  // Fetch PVEs
  // --------------------------------------------------
  @GET
  @Path("/pves")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def fetchPVEs(@QueryParam("cuid") cuid: Int): util.List[util.Map[String, Object]] = {
    try {
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

      resp

    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new InternalServerErrorException(s"Failed to get packages: ${e.getMessage}")
    }
  }
}
