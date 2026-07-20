/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.service.resource

import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs._
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.ComputingUnitConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.Tables.COMPUTING_UNIT_USER_ACCESS
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  ComputingUnitUserAccessDao,
  UserDao,
  WorkflowComputingUnitDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.ComputingUnitUserAccess
import org.apache.texera.service.resource.ComputingUnitAccessResource._
import org.jooq.{DSLContext, EnumType}

import scala.jdk.CollectionConverters._

object ComputingUnitAccessResource {
  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()

  /**
    * Identifies whether the given user has read-only access over the given computing unit
    *
    * @param cuid computing unit id
    * @param uid user id
    * @return boolean value indicating yes/no
    */
  def hasReadAccess(cuid: Integer, uid: Integer): Boolean = {
    isOwner(cuid, uid) || getPrivilege(cuid, uid).eq(PrivilegeEnum.READ) || hasWriteAccess(
      cuid,
      uid
    )
  }

  /**
    * Identifies whether the given user has write access over the given computing unit
    *
    * @param cuid computing unit id
    * @param uid user id
    * @return boolean value indicating yes/no
    */
  def hasWriteAccess(cuid: Integer, uid: Integer): Boolean = {
    isOwner(cuid, uid) || getPrivilege(cuid, uid).eq(PrivilegeEnum.WRITE)
  }

  /**
    * Identifies whether the given user is the owner of the given computing unit
    *
    * @param cuid computing unit id
    * @param uid user id
    * @return boolean value indicating yes/no
    */
  def isOwner(cuid: Integer, uid: Integer): Boolean = {
    val workflowComputingUnitDao = new WorkflowComputingUnitDao(context.configuration())
    val unit = workflowComputingUnitDao.fetchOneByCuid(cuid)
    unit != null && unit.getUid.equals(uid)
  }

  def getPrivilege(cuid: Integer, uid: Integer): PrivilegeEnum = {
    val computingUnitUserAccessDao = new ComputingUnitUserAccessDao(context.configuration())
    val accessList = computingUnitUserAccessDao
      .fetchByUid(uid)
      .asScala
      .find(_.getCuid.equals(cuid))

    accessList match {
      case Some(access) => access.getPrivilege
      case None         => null
    }
  }

  case class AccessEntry(email: String, name: String, privilege: EnumType) {}

}

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/access")
class ComputingUnitAccessResource {
  private def ensureSharingIsEnabled(): Unit = {
    if (!ComputingUnitConfig.sharingComputingUnitEnabled) {
      throw new ForbiddenException(
        "The computing unit sharing feature is disabled by the administrator."
      )
    }
  }
  final private val userDao = new UserDao(context.configuration())

  /**
    * Resolves an email to its user id, throwing a JAX-RS BadRequestException (400) when no
    * account matches — the service registers no ExceptionMapper for IllegalArgumentException,
    * so that would otherwise surface as an opaque HTTP 500. Shared by grant/revoke.
    */
  private def resolveUidByEmail(email: String): Integer = {
    val user = userDao.fetchOneByEmail(email)
    if (user == null) {
      throw new BadRequestException("User with the given email does not exist")
    }
    user.getUid
  }

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/computing-unit/list/{cuid}")
  def getComputingUnitAccessList(
      @Auth user: SessionUser,
      @PathParam("cuid") cuid: Integer
  ): List[AccessEntry] = {
    ensureSharingIsEnabled()
    withTransaction(context) { ctx =>
      val computingUnitUserAccessDao = new ComputingUnitUserAccessDao(ctx.configuration())
      computingUnitUserAccessDao
        .fetchByCuid(cuid)
        .asScala
        .map(access => {
          val user = userDao.fetchOneByUid(access.getUid)
          AccessEntry(
            email = user.getEmail,
            name = user.getName,
            privilege = access.getPrivilege
          )
        })
        .toList
    }
  }

  @PUT
  @Path("/computing-unit/grant/{cuid}/{email}/{privilege}")
  def grantAccess(
      @Auth user: SessionUser,
      @PathParam("cuid") cuid: Integer,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: PrivilegeEnum
  ): Unit = {
    ensureSharingIsEnabled()
    if (!hasWriteAccess(cuid, user.getUid)) {
      throw new ForbiddenException("User does not have permission to grant access")
    }

    val granteeId = resolveUidByEmail(email)

    withTransaction(context) { ctx =>
      val computingUnitUserAccessDao = new ComputingUnitUserAccessDao(ctx.configuration())
      val access = new ComputingUnitUserAccess
      access.setCuid(cuid)
      access.setUid(granteeId)
      access.setPrivilege(privilege)
      // merge (upsert) rather than insert: re-granting an existing grantee updates
      // their privilege in place instead of hitting a duplicate-primary-key error
      // (the (cuid, uid) PK). Mirrors DatasetAccessResource/WorkflowAccessResource.
      computingUnitUserAccessDao.merge(access)
    }
  }

  @DELETE
  @Path("/computing-unit/revoke/{cuid}/{email}")
  def revokeAccess(
      @Auth user: SessionUser,
      @PathParam("cuid") cuid: Integer,
      @PathParam("email") email: String
  ): Unit = {
    ensureSharingIsEnabled()
    if (!hasWriteAccess(cuid, user.getUid)) {
      throw new ForbiddenException("User does not have permission to revoke access")
    }

    val granteeId = resolveUidByEmail(email)

    withTransaction(context) { ctx =>
      ctx
        .delete(COMPUTING_UNIT_USER_ACCESS)
        .where(COMPUTING_UNIT_USER_ACCESS.CUID.eq(cuid))
        .and(COMPUTING_UNIT_USER_ACCESS.UID.eq(granteeId))
        .execute()
    }
  }

  @GET
  @Path("/computing-unit/owner/{cuid}")
  def getOwner(
      @Auth user: SessionUser,
      @PathParam("cuid") cuid: Integer
  ): String = {
    ensureSharingIsEnabled()

    withTransaction(context) { ctx =>
      val workflowComputingUnitDao = new WorkflowComputingUnitDao(ctx.configuration())
      val unit = workflowComputingUnitDao.fetchOneByCuid(cuid)
      if (unit == null) {
        // JAX-RS exception so it maps to 404: the service registers no ExceptionMapper
        // for IllegalArgumentException, which would otherwise surface as an HTTP 500.
        // Message style matches ComputingUnitManagingResource's nonexistent-unit error.
        throw new NotFoundException(s"Computing unit with cuid=$cuid does not exist.")
      }

      val uid = unit.getUid
      val owner = userDao.fetchOneByUid(uid)
      owner.getEmail
    }
  }
}
