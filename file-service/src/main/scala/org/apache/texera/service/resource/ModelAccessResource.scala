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
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.service.resource.ModelAccessResource.context
import org.apache.texera.service.resource.ResourceTables.{Model => MODEL_RESOURCE}
import org.jooq.DSLContext

object ModelAccessResource {
  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()

  type AccessEntry = ResourceAccess.AccessEntry
  val AccessEntry: ResourceAccess.AccessEntry.type = ResourceAccess.AccessEntry

  def isModelPublic(ctx: DSLContext, mid: Integer): Boolean =
    ResourceAccess.isPublic(ctx, MODEL_RESOURCE, mid)

  def userHasReadAccess(ctx: DSLContext, mid: Integer, uid: Integer): Boolean =
    ResourceAccess.userHasReadAccess(ctx, MODEL_RESOURCE, mid, uid)

  def userOwnModel(ctx: DSLContext, mid: Integer, uid: Integer): Boolean =
    ResourceAccess.userOwns(ctx, MODEL_RESOURCE, mid, uid)

  def userHasWriteAccess(ctx: DSLContext, mid: Integer, uid: Integer): Boolean =
    ResourceAccess.userHasWriteAccess(ctx, MODEL_RESOURCE, mid, uid)

  def getModelUserAccessPrivilege(
      ctx: DSLContext,
      mid: Integer,
      uid: Integer
  ): PrivilegeEnum = ResourceAccess.privilegeOf(ctx, MODEL_RESOURCE, mid, uid)

  def getOwner(ctx: DSLContext, mid: Integer): User =
    ResourceAccess.owner(ctx, MODEL_RESOURCE, mid)
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
  def getOwnerEmailOfModel(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): String =
    withTransaction(context)(ctx =>
      ResourceAccess.ownerEmail(ctx, MODEL_RESOURCE, mid, user.getUid)
    )

  /**
    * Returns information about all current shared access of the given model
    *
    * @param mid model id
    * @return a List of email/name/permission
    */
  @GET
  @Path("/list/{mid}")
  def getAccessList(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): java.util.List[ModelAccessResource.AccessEntry] =
    withTransaction(context)(ctx =>
      ResourceAccess.accessList(ctx, MODEL_RESOURCE, mid, user.getUid)
    )

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
  ): Response =
    withTransaction(context) { ctx =>
      ResourceAccess.grant(ctx, MODEL_RESOURCE, mid, email, privilege, user.getUid)
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
  ): Response =
    withTransaction(context) { ctx =>
      ResourceAccess.revoke(ctx, MODEL_RESOURCE, mid, email, user.getUid)
    }
}
