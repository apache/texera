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

package org.apache.texera.web.resource.dashboard.user.workflow

import io.dropwizard.auth.Auth
import org.apache.texera.auth.SessionUser
import org.apache.texera.auth.util.OwnerAccess
import org.apache.texera.auth.util.WorkflowAccess.{getPrivilege, hasWriteAccess, isOwner}
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.WorkflowUserAccess
import org.apache.texera.web.model.common.AccessEntry
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowAccessResource.context
import org.jooq.DSLContext

import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

object WorkflowAccessResource {
  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()
}

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/access/workflow")
class WorkflowAccessResource() {
  final private val userDao = new UserDao(context.configuration())
  final private val workflowOfUserDao = new WorkflowOfUserDao(context.configuration)
  final private val workflowUserAccessDao = new WorkflowUserAccessDao(context.configuration)

  /**
    * This method returns the owner of a workflow
    *
    * @param wid ,  workflow id
    * @return ownerEmail,  the owner's email
    */
  @GET
  @Path("/owner/{wid}")
  def getOwner(@PathParam("wid") wid: Integer): String = {
    userDao.fetchOneByUid(workflowOfUserDao.fetchByWid(wid).get(0).getUid).getEmail
  }

  /**
    * Returns information about all current shared access of the given workflow
    *
    * @param wid workflow id
    * @return a List of email/name/permission
    */
  @GET
  @Path("/list/{wid}")
  def getAccessList(
      @PathParam("wid") wid: Integer
  ): util.List[AccessEntry] = {
    context
      .select(
        USER.EMAIL,
        USER.NAME,
        WORKFLOW_USER_ACCESS.PRIVILEGE
      )
      .from(WORKFLOW_USER_ACCESS)
      .join(USER)
      .on(USER.UID.eq(WORKFLOW_USER_ACCESS.UID))
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(wid)
          .and(WORKFLOW_USER_ACCESS.UID.notEqual(workflowOfUserDao.fetchByWid(wid).get(0).getUid))
      )
      .fetchInto(classOf[AccessEntry])
  }

  /**
    * This method shares a workflow to a user with a specific access type
    *
    * @param wid       the given workflow
    * @param email     the email which the access is given to
    * @param privilege the type of Access given to the target user
    * @return rejection if user not permitted to share the workflow or Success Message
    */
  @PUT
  @Path("/grant/{wid}/{email}/{privilege}")
  def grantAccess(
      @PathParam("wid") wid: Integer,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: String,
      @Auth user: SessionUser
  ): Unit = {
    val isModifyingOwnAccess = email.equals(user.getEmail)
    val currentPrivilege = getPrivilege(wid, user.getUid)
    val hasExistingAccess = !currentPrivilege.eq(PrivilegeEnum.NONE)

    // Users can only modify their own access if they already have access
    if (isModifyingOwnAccess && !hasExistingAccess) {
      throw new BadRequestException("You cannot grant access to yourself!")
    }

    // Must have write access to modify access levels (including your own)
    if (!hasWriteAccess(wid, user.getUid)) {
      throw new ForbiddenException(s"You do not have permission to modify workflow $wid")
    }

    val targetUser = userDao.fetchOneByEmail(email)
    if (targetUser == null || targetUser.getIsPlaceholder) {
      throw new BadRequestException(s"No registered user with email $email")
    }
    val userUid = targetUser.getUid
    if (isOwner(wid, userUid)) {
      // amber is on javax JAX-RS, so it raises its own ForbiddenException with the shared message.
      throw new ForbiddenException(OwnerAccess.ownerChangeRefused)
    }

    try {
      workflowUserAccessDao.merge(
        new WorkflowUserAccess(
          userUid,
          wid,
          PrivilegeEnum.valueOf(privilege)
        )
      )
    } catch {
      case _: NullPointerException =>
        throw new BadRequestException(s"User $email Not Found!")
    }
  }

  /**
    * This method identifies the user access level of the given workflow
    *
    * @param wid   the given workflow
    * @param email the email of the use whose access is about to be removed
    * @return message indicating a success message
    */
  @DELETE
  @Path("/revoke/{wid}/{email}")
  def revokeAccess(
      @PathParam("wid") wid: Integer,
      @PathParam("email") email: String,
      @Auth user: SessionUser
  ): Unit = {
    try {
      val targetUserUid = userDao.fetchOneByEmail(email).getUid

      // Prevent owner from revoking their own access
      if (isOwner(wid, targetUserUid)) {
        throw new ForbiddenException("The owner cannot revoke their own access")
      }

      // Allow if: (1) user has WRITE access, OR (2) user is revoking their own access
      val isRevokingOwnAccess = targetUserUid == user.getUid
      if (!hasWriteAccess(wid, user.getUid) && !isRevokingOwnAccess) {
        throw new ForbiddenException(s"You do not have permission to modify workflow $wid")
      }

      context
        .delete(WORKFLOW_USER_ACCESS)
        .where(
          WORKFLOW_USER_ACCESS.UID
            .eq(targetUserUid)
            .and(WORKFLOW_USER_ACCESS.WID.eq(wid))
        )
        .execute()
    } catch {
      case _: NullPointerException =>
        throw new BadRequestException(s"User $email Not Found!")
    }
  }
}
