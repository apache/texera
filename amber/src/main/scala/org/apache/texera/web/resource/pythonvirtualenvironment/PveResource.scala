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

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters._
import java.util

@Path("/pve")
@Consumes(Array(MediaType.APPLICATION_JSON))
class PveResource {
  // --------------------------------------------------
  // Get installed packages
  // --------------------------------------------------
  @GET
  @Path("/system")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getSystemPackages: util.Map[String, util.List[String]] = {
    try {
      val systemPkgs = PveManager.getSystemPackages().toList.asJava
      Map("system" -> systemPkgs).asJava
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new InternalServerErrorException("Failed to get system packages.")
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
      PveManager
        .getEnvironments(cuid)
        .map { pveName =>
          Map(
            "pveName" -> pveName.asInstanceOf[Object]
          ).asJava
        }
        .asJava

    } catch {
      case e: Exception =>
        e.printStackTrace()
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
}
