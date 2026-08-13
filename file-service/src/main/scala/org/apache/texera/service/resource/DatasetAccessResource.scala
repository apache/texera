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
import org.apache.texera.service.resource.DatasetAccessResource.context
import org.apache.texera.service.resource.ManagedResource.{Dataset => DATASET_RESOURCE}
import org.jooq.DSLContext

object DatasetAccessResource {
  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()

  type AccessEntry = ResourceAccess.AccessEntry
  val AccessEntry: ResourceAccess.AccessEntry.type = ResourceAccess.AccessEntry

  def isDatasetPublic(ctx: DSLContext, did: Integer): Boolean =
    ResourceAccess.isPublic(ctx, DATASET_RESOURCE, did)

  def userHasReadAccess(ctx: DSLContext, did: Integer, uid: Integer): Boolean =
    ResourceAccess.userHasReadAccess(ctx, DATASET_RESOURCE, did, uid)

  def userOwnDataset(ctx: DSLContext, did: Integer, uid: Integer): Boolean =
    ResourceAccess.userOwns(ctx, DATASET_RESOURCE, did, uid)

  def userHasWriteAccess(ctx: DSLContext, did: Integer, uid: Integer): Boolean =
    ResourceAccess.userHasWriteAccess(ctx, DATASET_RESOURCE, did, uid)

  def getDatasetUserAccessPrivilege(
      ctx: DSLContext,
      did: Integer,
      uid: Integer
  ): PrivilegeEnum = ResourceAccess.privilegeOf(ctx, DATASET_RESOURCE, did, uid)

  def getOwner(ctx: DSLContext, did: Integer): User =
    ResourceAccess.owner(ctx, DATASET_RESOURCE, did)
}

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/access/dataset")
class DatasetAccessResource {

  /**
    * This method returns the owner of a dataset
    *
    * @param did ,  dataset id
    * @return ownerEmail,  the owner's email
    */
  @GET
  @Path("/owner/{did}")
  def getOwnerEmailOfDataset(@PathParam("did") did: Integer): String =
    withTransaction(context)(ctx => ResourceAccess.ownerEmail(ctx, DATASET_RESOURCE, did))

  /**
    * Returns information about all current shared access of the given dataset
    *
    * @param did dataset id
    * @return a List of email/name/permission
    */
  @GET
  @Path("/list/{did}")
  def getAccessList(
      @PathParam("did") did: Integer
  ): java.util.List[DatasetAccessResource.AccessEntry] =
    withTransaction(context)(ctx => ResourceAccess.accessList(ctx, DATASET_RESOURCE, did))

  /**
    * This method shares a dataset to a user with a specific access type
    *
    * @param did       the given dataset
    * @param email     the email which the access is given to
    * @param privilege the type of Access given to the target user
    * @return rejection if user not permitted to share the workflow or Success Message
    */
  @PUT
  @Path("/grant/{did}/{email}/{privilege}")
  def grantAccess(
      @PathParam("did") did: Integer,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: String,
      @Auth user: SessionUser
  ): Response =
    withTransaction(context) { ctx =>
      ResourceAccess.grant(ctx, DATASET_RESOURCE, did, email, privilege, user.getUid)
    }

  /**
    * This method revoke the user's access of the given dataset
    *
    * @param did   the given dataset
    * @param email the email of the use whose access is about to be removed
    * @return message indicating a success message
    */
  @DELETE
  @Path("/revoke/{did}/{email}")
  def revokeAccess(
      @PathParam("did") did: Integer,
      @PathParam("email") email: String,
      @Auth user: SessionUser
  ): Response =
    withTransaction(context) { ctx =>
      ResourceAccess.revoke(ctx, DATASET_RESOURCE, did, email, user.getUid)
    }
}
