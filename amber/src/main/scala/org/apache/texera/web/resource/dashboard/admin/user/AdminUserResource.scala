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

package org.apache.texera.web.resource.dashboard.admin.user

import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.AuthProvider.AUTH_PROVIDER
import org.apache.texera.dao.jooq.generated.tables.UserLastActiveTime.USER_LAST_ACTIVE_TIME
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.EmailTemplate.createRoleChangeTemplate
import org.apache.texera.web.resource.GmailResource.sendEmail
import org.apache.texera.web.resource.auth.LocalAuthProvisioner
import org.apache.texera.web.resource.dashboard.admin.user.AdminUserResource.userDao
import org.apache.texera.web.resource.dashboard.user.dataset.utils.DatasetStatisticsUtils.getUserCreatedDatasets
import org.apache.texera.web.resource.dashboard.user.quota.UserQuotaResource._

import java.util
import java.util.UUID
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

case class UserInfo(
    uid: Int,
    name: String,
    email: String,
    googleId: String,
    role: UserRoleEnum,
    avatar: String,
    comment: String,
    lastLogin: java.time.OffsetDateTime, // will be null if never logged in
    accountCreation: java.time.OffsetDateTime,
    affiliation: String,
    joiningReason: String,
    isPlaceholder: Boolean
)

object AdminUserResource {
  private def context =
    SqlServer
      .getInstance()
      .createDSLContext()
  private def userDao = new UserDao(context.configuration)
}

@Path("/admin/user")
@RolesAllowed(Array("ADMIN"))
class AdminUserResource {

  /**
    * This method returns the list of users
    *
    * @return a list of UserInfo
    */
  @GET
  @Path("/list")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def list(): util.List[UserInfo] = {

    val googleProvider = AUTH_PROVIDER.as("google_provider")

    AdminUserResource.context
      .select(
        USER.UID,
        USER.NAME,
        USER.EMAIL,
        // fetchInto maps onto a Scala case class POSITIONALLY, not by name: a case class has no
        // no-arg constructor, so jOOQ falls through to ImmutablePOJOMapper. So the column order
        // below must track the UserInfo field order — adding, removing or reordering a projected
        // column here without doing the same to UserInfo silently shifts every later field.
        // `last_active_time` landing on `lastLogin` only works because of that. The aliases are
        // documentation; they do not drive the mapping.
        googleProvider.PROVIDER_ID.as("googleId"),
        USER.ROLE,
        USER.AVATAR,
        USER.COMMENT,
        USER_LAST_ACTIVE_TIME.LAST_ACTIVE_TIME,
        USER.ACCOUNT_CREATION_TIME,
        USER.AFFILIATION,
        USER.JOINING_REASON,
        USER.IS_PLACEHOLDER
      )
      .from(USER)
      .leftJoin(USER_LAST_ACTIVE_TIME)
      .on(USER.UID.eq(USER_LAST_ACTIVE_TIME.UID))
      .leftJoin(googleProvider)
      .on(googleProvider.PROVIDER_TYPE.eq(ProviderTypeEnum.GOOGLE))
      .and(googleProvider.UID.eq(USER.UID))
      .fetchInto(classOf[UserInfo])
  }

  @PUT
  @Path("/update")
  def updateUser(user: User): Unit = {
    val existingUser = userDao.fetchOneByEmail(user.getEmail)
    if (existingUser != null && existingUser.getUid != user.getUid) {
      throw new WebApplicationException("Email already exists", Response.Status.CONFLICT)
    }
    val updatedUser = userDao.fetchOneByUid(user.getUid)
    val roleChanged = updatedUser.getRole != user.getRole
    updatedUser.setName(user.getName)
    updatedUser.setEmail(user.getEmail)
    updatedUser.setRole(user.getRole)
    updatedUser.setComment(user.getComment)
    userDao.update(updatedUser)

    if (roleChanged)
      sendEmail(
        createRoleChangeTemplate(receiverEmail = updatedUser.getEmail, newRole = user.getRole),
        updatedUser.getEmail
      )
  }

  @POST
  @Path("/add")
  def addUser(): Unit = {
    // Two independent UUIDs: the handle is visible to anyone who can read /list, so deriving the
    // password from it would let any such caller log in as the new account.
    val handle = "User" + UUID.randomUUID().toString
    val user = new User
    user.setName(handle)
    user.setRole(UserRoleEnum.INACTIVE)
    LocalAuthProvisioner.createLocalAccount(user, handle, UUID.randomUUID().toString)
  }

  @GET
  @Path("/created_datasets")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCreatedDatasets(@QueryParam("user_id") user_id: Integer): List[DatasetQuota] = {
    if (user_id == null) {
      throw new BadRequestException("user_id is required")
    }
    getUserCreatedDatasets(user_id)
  }

  @GET
  @Path("/created_workflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCreatedWorkflow(@QueryParam("user_id") user_id: Integer): List[Workflow] = {
    getUserCreatedWorkflow(user_id)
  }

  @GET
  @Path("/access_workflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccessedWorkflow(@QueryParam("user_id") user_id: Integer): util.List[Integer] = {
    getUserAccessedWorkflow(user_id)
  }

  @GET
  @Path("/user_quota_size")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getUserQuota(@QueryParam("user_id") user_id: Integer): Array[QuotaStorage] = {
    getUserQuotaSize(user_id)
  }

  @DELETE
  @Path("/deleteCollection/{eid}")
  def deleteCollection(@PathParam("eid") eid: Integer): Unit = {
    deleteExecutionCollection(eid)
  }
}
