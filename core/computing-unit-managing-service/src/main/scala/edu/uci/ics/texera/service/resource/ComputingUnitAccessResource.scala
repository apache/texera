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

package edu.uci.ics.texera.service.resource

import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.config.ComputingUnitConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{
  ComputingUnitUserAccessDao,
  UserDao,
  WorkflowComputingUnitDao
}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.ComputingUnitUserAccess
import edu.uci.ics.texera.service.resource.ComputingUnitAccessResource._
import edu.uci.ics.texera.dao.jooq.generated.Tables.COMPUTING_UNIT_USER_ACCESS

import scala.jdk.CollectionConverters._
import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{DELETE, ForbiddenException, GET, PUT, Path, PathParam, Produces}
import org.jooq.{DSLContext, EnumType}

object ComputingUnitAccessResource {
  private lazy val context: DSLContext = SqlServer
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
      throw new IllegalArgumentException("User does not have permission to grant access")
    }

    // TODO: add try except and check how to display error message in the frontend
    val granteeId = userDao.fetchOneByEmail(email).getUid
    if (granteeId == null) {
      throw new IllegalArgumentException("User with the given email does not exist")
    }

    withTransaction(context) { ctx =>
      val computingUnitUserAccessDao = new ComputingUnitUserAccessDao(ctx.configuration())
      val access = new ComputingUnitUserAccess
      access.setCuid(cuid)
      access.setUid(granteeId)
      access.setPrivilege(privilege)
      computingUnitUserAccessDao.insert(access)
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
      throw new IllegalArgumentException("User does not have permission to revoke access")
    }

    val granteeId = userDao.fetchOneByEmail(email).getUid
    if (granteeId == null) {
      throw new IllegalArgumentException("User with the given email does not exist")
    }

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
        throw new IllegalArgumentException("Computing unit does not exist")
      }

      val uid = unit.getUid
      val owner = userDao.fetchOneByUid(uid)
      owner.getEmail
    }
  }
}
