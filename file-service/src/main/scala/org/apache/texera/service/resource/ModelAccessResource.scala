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

import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.core.{MediaType, Response}
import jakarta.ws.rs._
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.Tables.USER
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.ModelUserAccess.MODEL_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.daos.{ModelDao, ModelUserAccessDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{ModelUserAccess, User}
import org.apache.texera.service.resource.ModelAccessResource.{
  AccessEntry,
  context,
  getOwner,
  userHasWriteAccess
}
import org.jooq.{DSLContext, EnumType}

object ModelAccessResource {
  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()

  def isModelPublic(ctx: DSLContext, mid: Integer): Boolean = {
    val modelDao = new ModelDao(ctx.configuration())
    Option(modelDao.fetchOneByMid(mid))
      .flatMap(model => Option(model.getIsPublic))
      .contains(true)
  }

  def userHasReadAccess(ctx: DSLContext, mid: Integer, uid: Integer): Boolean = {
    isModelPublic(ctx, mid) ||
    userHasWriteAccess(ctx, mid, uid) ||
    getModelUserAccessPrivilege(ctx, mid, uid) == PrivilegeEnum.READ
  }

  def userOwnModel(ctx: DSLContext, mid: Integer, uid: Integer): Boolean = {
    val modelDao = new ModelDao(ctx.configuration())

    Option(modelDao.fetchOneByMid(mid))
      .exists(_.getOwnerUid == uid)
  }

  def userHasWriteAccess(ctx: DSLContext, mid: Integer, uid: Integer): Boolean = {
    userOwnModel(ctx, mid, uid) ||
    getModelUserAccessPrivilege(ctx, mid, uid) == PrivilegeEnum.WRITE
  }

  def getModelUserAccessPrivilege(
      ctx: DSLContext,
      mid: Integer,
      uid: Integer
  ): PrivilegeEnum = {
    Option(
      ctx
        .select(MODEL_USER_ACCESS.PRIVILEGE)
        .from(MODEL_USER_ACCESS)
        .where(
          MODEL_USER_ACCESS.MID
            .eq(mid)
            .and(MODEL_USER_ACCESS.UID.eq(uid))
        )
        .fetchOneInto(classOf[PrivilegeEnum])
    ).getOrElse(PrivilegeEnum.NONE)
  }

  def getOwner(ctx: DSLContext, mid: Integer): User = {
    val modelDao = new ModelDao(ctx.configuration())
    val userDao = new UserDao(ctx.configuration())

    Option(modelDao.fetchOneByMid(mid))
      .flatMap(model => Option(model.getOwnerUid))
      .map(ownerUid => userDao.fetchOneByUid(ownerUid))
      .orNull
  }

  case class AccessEntry(email: String, name: String, privilege: EnumType) {}

}

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/access/model")
class ModelAccessResource {

  /**
    * This method returns the owner of a model
    *
    * @param mid ,  model id
    * @return ownerEmail,  the owner's email
    */
  @GET
  @Path("/owner/{mid}")
  def getOwnerEmailOfModel(@PathParam("mid") mid: Integer): String = {
    var email = ""
    withTransaction(context) { ctx =>
      val owner = getOwner(ctx, mid)
      if (owner != null) {
        email = owner.getEmail
      }
    }
    email
  }

  /**
    * Returns information about all current shared access of the given model
    *
    * @param mid model id
    * @return a List of email/name/permission
    */
  @GET
  @Path("/list/{mid}")
  def getAccessList(
      @PathParam("mid") mid: Integer
  ): java.util.List[AccessEntry] = {
    withTransaction(context) { ctx =>
      val modelDao = new ModelDao(ctx.configuration())
      ctx
        .select(
          USER.EMAIL,
          USER.NAME,
          MODEL_USER_ACCESS.PRIVILEGE
        )
        .from(MODEL_USER_ACCESS)
        .join(USER)
        .on(USER.UID.eq(MODEL_USER_ACCESS.UID))
        .where(
          MODEL_USER_ACCESS.MID
            .eq(mid)
            .and(MODEL_USER_ACCESS.UID.notEqual(modelDao.fetchOneByMid(mid).getOwnerUid))
        )
        .fetchInto(classOf[AccessEntry])
    }
  }

  /**
    * This method shares a model to a user with a specific access type
    *
    * @param mid       the given model
    * @param email     the email which the access is given to
    * @param privilege the type of Access given to the target user
    * @return rejection if user not permitted to share the model or Success Message
    */
  @PUT
  @Path("/grant/{mid}/{email}/{privilege}")
  def grantAccess(
      @PathParam("mid") mid: Integer,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: String,
      @Auth user: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, user.getUid)) {
        throw new ForbiddenException(s"You do not have permission to modify model $mid")
      }
      val modelUserAccessDao = new ModelUserAccessDao(ctx.configuration())
      val userDao = new UserDao(ctx.configuration())
      modelUserAccessDao.merge(
        new ModelUserAccess(
          mid,
          userDao.fetchOneByEmail(email).getUid,
          PrivilegeEnum.valueOf(privilege)
        )
      )
      Response.ok().build()
    }
  }

  /**
    * This method revoke the user's access of the given model
    *
    * @param mid   the given model
    * @param email the email of the use whose access is about to be removed
    * @return message indicating a success message
    */
  @DELETE
  @Path("/revoke/{mid}/{email}")
  def revokeAccess(
      @PathParam("mid") mid: Integer,
      @PathParam("email") email: String,
      @Auth user: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, user.getUid)) {
        throw new ForbiddenException(s"You do not have permission to modify model $mid")
      }

      val userDao = new UserDao(ctx.configuration())

      ctx
        .delete(MODEL_USER_ACCESS)
        .where(
          MODEL_USER_ACCESS.UID
            .eq(userDao.fetchOneByEmail(email).getUid)
            .and(MODEL_USER_ACCESS.MID.eq(mid))
        )
        .execute()

      Response.ok().build()
    }
  }
}
