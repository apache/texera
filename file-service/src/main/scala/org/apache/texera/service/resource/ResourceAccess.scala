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

import jakarta.ws.rs.{BadRequestException, ForbiddenException}
import jakarta.ws.rs.core.Response
import org.apache.texera.dao.jooq.generated.Tables.USER
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.jooq.{DSLContext, EnumType, Record}

/**
  * Ownership and privilege rules shared by every access-controlled resource.
  *
  * A resource is readable when it is public, or the caller owns it, or the caller holds an
  * explicit grant; it is writable when the caller owns it or holds a WRITE grant. A missing
  * resource resolves to "not public, unowned, ungranted" rather than an error, so callers
  * decide whether absence is a 403 or a 404.
  */
object ResourceAccess {

  /** One shared grant, as returned by the access-list endpoints. */
  case class AccessEntry(email: String, name: String, privilege: EnumType) {}

  def isPublic[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer
  ): Boolean =
    Option(
      ctx
        .select(resource.isPublicField)
        .from(resource.table)
        .where(resource.idField.eq(id))
        .fetchOne()
    ).flatMap(record => Option(record.value1()))
      .exists(_.booleanValue())

  def userOwns[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer,
      uid: Integer
  ): Boolean =
    Option(
      ctx
        .select(resource.ownerUidField)
        .from(resource.table)
        .where(resource.idField.eq(id))
        .fetchOne()
    ).flatMap(record => Option(record.value1()))
      .contains(uid)

  def privilegeOf[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer,
      uid: Integer
  ): PrivilegeEnum =
    Option(
      ctx
        .select(resource.privilegeField)
        .from(resource.accessTable)
        .where(
          resource.accessIdField
            .eq(id)
            .and(resource.accessUidField.eq(uid))
        )
        .fetchOneInto(classOf[PrivilegeEnum])
    ).getOrElse(PrivilegeEnum.NONE)

  def userHasWriteAccess[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer,
      uid: Integer
  ): Boolean =
    userOwns(ctx, resource, id, uid) ||
      privilegeOf(ctx, resource, id, uid) == PrivilegeEnum.WRITE

  def userHasReadAccess[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer,
      uid: Integer
  ): Boolean =
    isPublic(ctx, resource, id) ||
      userHasWriteAccess(ctx, resource, id, uid) ||
      privilegeOf(ctx, resource, id, uid) == PrivilegeEnum.READ

  /** The owning user, or null when the resource does not exist. */
  def owner[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer
  ): User = {
    val userDao = new UserDao(ctx.configuration())
    Option(
      ctx
        .select(resource.ownerUidField)
        .from(resource.table)
        .where(resource.idField.eq(id))
        .fetchOne()
    ).flatMap(record => Option(record.value1()))
      .map(ownerUid => userDao.fetchOneByUid(ownerUid))
      .orNull
  }

  /** The owner's email, or an empty string when the resource does not exist. */
  def ownerEmail[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer
  ): String = Option(owner(ctx, resource, id)).map(_.getEmail).getOrElse("")

  /** Everyone the resource is shared with, excluding the owner's own row. */
  def accessList[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer
  ): java.util.List[AccessEntry] = {
    val ownerUid = ctx
      .select(resource.ownerUidField)
      .from(resource.table)
      .where(resource.idField.eq(id))
      .fetchOne()
      .value1()

    ctx
      .select(USER.EMAIL, USER.NAME, resource.privilegeField)
      .from(resource.accessTable)
      .join(USER)
      .on(USER.UID.eq(resource.accessUidField))
      .where(
        resource.accessIdField
          .eq(id)
          .and(resource.accessUidField.notEqual(ownerUid))
      )
      .fetchInto(classOf[AccessEntry])
  }

  /**
    * Grants `privilege` to the user with `email`, replacing any privilege they already hold.
    *
    * Placeholder accounts stand in for people who were referenced by email but never signed up,
    * so they cannot be shared with.
    */
  def grant[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer,
      email: String,
      privilege: String,
      requesterUid: Integer
  ): Response = {
    requireWriteAccess(ctx, resource, id, requesterUid)
    val grantee = new UserDao(ctx.configuration()).fetchOneByEmail(email)
    if (grantee == null || grantee.getIsPlaceholder) {
      throw new BadRequestException(s"No registered user with email $email")
    }
    val granteeUid = grantee.getUid
    val granted = PrivilegeEnum.valueOf(privilege)

    ctx
      .insertInto(resource.accessTable)
      .set(resource.accessIdField, id)
      .set(resource.accessUidField, granteeUid)
      .set(resource.privilegeField, granted)
      .onConflict(resource.accessIdField, resource.accessUidField)
      .doUpdate()
      .set(resource.privilegeField, granted)
      .execute()

    Response.ok().build()
  }

  /** Removes the user's explicit grant; a no-op when they hold none. */
  def revoke[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer,
      email: String,
      requesterUid: Integer
  ): Response = {
    requireWriteAccess(ctx, resource, id, requesterUid)
    val granteeUid = new UserDao(ctx.configuration()).fetchOneByEmail(email).getUid

    ctx
      .delete(resource.accessTable)
      .where(
        resource.accessUidField
          .eq(granteeUid)
          .and(resource.accessIdField.eq(id))
      )
      .execute()

    Response.ok().build()
  }

  private def requireWriteAccess[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ManagedResource[R, A],
      id: Integer,
      uid: Integer
  ): Unit =
    if (!userHasWriteAccess(ctx, resource, id, uid)) {
      throw new ForbiddenException(
        s"You do not have permission to modify ${resource.label} $id"
      )
    }
}
