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
import jakarta.ws.rs.core.MediaType
import org.apache.texera.auth.JwtAuth.{jwtClaims, jwtToken}
import org.apache.texera.auth.SessionUser
import org.apache.texera.auth.util.ComputingUnitAccess
import org.apache.texera.common.config.{EnvironmentalVariable, KubernetesConfig}
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.Tables.{DATASET_VERSION, MODEL_VERSION}
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, ModelDao}
import org.apache.texera.service.resource.ComputingUnitMountResource._
import org.apache.texera.service.util.{
  ComputingUnitNodeLocator,
  MountRequestValidation,
  MounterClient
}

import scala.jdk.CollectionConverters._

/**
  * The mount authority: this service decides whether a user may act on a computing unit, so
  * it is where a mount request is authorized before being forwarded to that unit's node.
  */
@Path("/mounts")
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Produces(Array(MediaType.APPLICATION_JSON))
class ComputingUnitMountResource(
    mounterEnabled: Boolean,
    mounterPort: Option[Int],
    fileServiceBaseUrl: Option[String],
    nodeLocator: ComputingUnitNodeLocator,
    mounter: MounterClient
) extends LazyLogging {

  // No-arg constructor for Jersey reflection. Tests use the param-ful form.
  def this() =
    this(
      KubernetesConfig.mounterEnabled,
      EnvironmentalVariable.get(MounterPortVariable).map(_.trim.toInt),
      EnvironmentalVariable.get(FileServiceUrlVariable),
      ComputingUnitNodeLocator,
      MounterClient
    )

  @POST
  @Path("/{cuid}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def mount(
      @PathParam("cuid") cuid: Int,
      request: MountRequest,
      @Auth user: SessionUser
  ): MountInfo = {
    val (port, fileService) = requireMountConfiguration()
    try MountRequestValidation.validate(cuid.toString, request.repositoryName, request.commitHash)
    catch { case e: IllegalArgumentException => throw new BadRequestException(e.getMessage) }
    requireComputingUnitAccess(cuid, user)
    requireRepositoryReadAccess(request.repositoryName, request.commitHash, user.getUid)
    val nodeIp = requireNodeIp(cuid)

    // A token minted here, after the access check: GeeseFS keeps presenting it for the life
    // of the mount, so it must be one this service vouched for.
    val mountPath =
      try {
        mounter.mount(
          nodeIp,
          port,
          cuid.toString,
          request.repositoryName,
          request.commitHash,
          jwtToken(jwtClaims(user.getUser)),
          fileService
        )
      } catch {
        case e: IllegalArgumentException =>
          throw new BadRequestException(e.getMessage)
        case e: MounterClient.MounterRequestException =>
          logger.warn(s"node mounter at $nodeIp refused a mount for computing unit $cuid", e)
          throw new BadRequestException(e.getMessage)
      }

    logger.info(
      s"user ${user.getUid} mounted ${request.repositoryName}:${request.commitHash} " +
        s"onto computing unit $cuid at $mountPath"
    )
    MountInfo(request.repositoryName, request.commitHash, mountPath)
  }

  /**
    * What a mount request needs beyond the request itself, both passed by the chart. Missing
    * with mounting off is said plainly, because the alternative is a connection timeout to a
    * node port nothing is listening on; missing with it on is a misconfiguration, and naming
    * the variable beats letting a half-formed request reach the mounter.
    */
  private def requireMountConfiguration(): (Int, String) = {
    if (!mounterEnabled) {
      throw new ServiceUnavailableException(
        "Repository mounting is not enabled on this deployment."
      )
    }
    def required[T](value: Option[T], variable: String): T =
      value.getOrElse(
        throw new InternalServerErrorException(
          s"Repository mounting is enabled but $variable is unset."
        )
      )
    (
      required(mounterPort, MounterPortVariable),
      required(fileServiceBaseUrl.filter(_.nonEmpty), FileServiceUrlVariable)
    )
  }

  /**
    * Mounting puts data into someone's computing unit, so it takes the same privilege as any
    * other change to one: ownership, or an explicit WRITE grant. A read-only sharee may use
    * the unit, not alter what it can see.
    */
  private def requireComputingUnitAccess(cuid: Int, user: SessionUser): Unit =
    if (ComputingUnitAccess.getComputingUnitAccess(cuid, user.getUid) != PrivilegeEnum.WRITE) {
      logger.warn(s"user ${user.getUid} denied mount access to computing unit $cuid")
      throw new ForbiddenException("No write access to this computing unit.")
    }

  /**
    * The repository must be one the user may read, at a commit that belongs to it.
    *
    * The mounter performs what it is told and authorizes nothing, so this is where a mount is
    * refused. file-service re-checks read access on every byte served through its proxy, but
    * that is the last line rather than this one: without the check here a caller could have a
    * mount created for a repository they cannot read, learning it exists and spending a node's
    * resources on it.
    *
    * A repository is matched by name rather than parsed, because `sql/updates/15.sql`
    * backfilled the column from the dataset's plain name; both resource kinds are searched,
    * and anything other than exactly one match is refused rather than resolved arbitrarily.
    */
  private def requireRepositoryReadAccess(
      repositoryName: String,
      commitHash: String,
      uid: Integer
  ): Unit =
    withTransaction(SqlServer.getInstance().createDSLContext()) { ctx =>
      val datasets = new DatasetDao(ctx.configuration()).fetchByRepositoryName(repositoryName)
      val models = new ModelDao(ctx.configuration()).fetchByRepositoryName(repositoryName)

      val readable = (datasets.asScala.toList, models.asScala.toList) match {
        case (dataset :: Nil, Nil) =>
          ResourceAccess.userHasReadAccess(ctx, ResourceTables.Dataset, dataset.getDid, uid) &&
            ctx.fetchExists(
              DATASET_VERSION,
              DATASET_VERSION.DID
                .eq(dataset.getDid)
                .and(DATASET_VERSION.VERSION_HASH.eq(commitHash))
            )
        case (Nil, model :: Nil) =>
          ResourceAccess.userHasReadAccess(ctx, ResourceTables.Model, model.getMid, uid) &&
            ctx.fetchExists(
              MODEL_VERSION,
              MODEL_VERSION.MID.eq(model.getMid).and(MODEL_VERSION.VERSION_HASH.eq(commitHash))
            )
        case _ => false
      }

      if (!readable) {
        logger.warn(s"user $uid denied a mount of '$repositoryName' at '$commitHash'")
        throw new ForbiddenException("No read access to the requested repository version.")
      }
    }

  private def requireNodeIp(cuid: Int): String =
    nodeLocator
      .nodeIpOf(cuid)
      .getOrElse(
        throw new BadRequestException(
          s"Computing unit $cuid is not running on a node yet; cannot manage its mounts."
        )
      )
}

object ComputingUnitMountResource {

  /** Set by the chart from the same value it gives the mounter DaemonSet's hostPort. */
  private val MounterPortVariable = "KUBERNETES_MOUNTER_PORT"

  // file-service's root, which is what GeeseFS is pointed at: the S3 proxy is served at the
  // servlet root, so this is scheme and authority with no path.
  private val FileServiceUrlVariable = "FILE_SERVICE_URL"

  case class MountRequest(repositoryName: String, commitHash: String)

  case class MountInfo(repositoryName: String, commitHash: String, mountPath: String)
}
