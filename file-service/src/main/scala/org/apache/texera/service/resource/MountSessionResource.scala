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

import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs._
import jakarta.ws.rs.core.{MediaType, Response}
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.tables.daos.DatasetDao
import org.apache.texera.service.resource.DatasetAccessResource.userHasReadAccess
import org.apache.texera.service.util.MountSessionStore

import scala.jdk.CollectionConverters._

/**
  * Issues short-lived, single-dataset-scoped credentials that let a computing-unit pod's
  * GeeseFS mount reach the LakeFS S3 gateway through [[org.apache.texera.service.util.S3ProxyServlet]]
  * without ever receiving the global LakeFS credentials. The caller authenticates with
  * the per-user JWT the pod already holds; access is checked with the same
  * `userHasReadAccess` gate used by the dataset presign endpoints.
  */
@Path("/dataset")
@Produces(Array(MediaType.APPLICATION_JSON))
class MountSessionResource extends LazyLogging {

  private def context = SqlServer.getInstance().createDSLContext()

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/mount-session")
  def createMountSession(
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response = {
    if (
      repositoryName == null || repositoryName.isEmpty || commitHash == null || commitHash.isEmpty
    ) {
      return Response
        .status(Response.Status.BAD_REQUEST)
        .entity(Map("error" -> "repositoryName and commitHash are required"))
        .build()
    }

    val uid = user.getUid
    val authorized = withTransaction(context) { ctx =>
      val datasets = new DatasetDao(ctx.configuration())
        .fetchByRepositoryName(repositoryName)
        .asScala
        .toList
      datasets.nonEmpty && userHasReadAccess(ctx, datasets.head.getDid, uid)
    }

    if (!authorized) {
      return Response
        .status(Response.Status.FORBIDDEN)
        .entity(Map("error" -> "no read access to the requested dataset"))
        .build()
    }

    val session =
      MountSessionStore.create(repositoryName, commitHash, uid, System.currentTimeMillis())
    logger.info(
      s"issued mount session for user $uid on $repositoryName@$commitHash (accessKey ${session.accessKey})"
    )

    Response
      .ok(
        Map(
          "accessKey" -> session.accessKey,
          "secretKey" -> session.secretKey,
          "expiresInSeconds" -> (MountSessionStore.TtlMillis / 1000)
        )
      )
      .build()
  }
}
