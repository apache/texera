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

import scala.jdk.CollectionConverters._

/**
  * Ownership and privilege rules shared by every access-controlled resource.
  *
  * A resource is readable when it is public, or the caller owns it, or the caller holds an
  * explicit grant; it is writable when the caller owns it or holds a WRITE grant.
  */
object ResourceAccess {

  /** One shared grant, as returned by the access-list endpoints. */
  case class AccessEntry(email: String, name: String, privilege: EnumType) {}

  def isPublic[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
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
      resource: ResourceTables[R, A],
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
      resource: ResourceTables[R, A],
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
      resource: ResourceTables[R, A],
      id: Integer,
      uid: Integer
  ): Boolean =
    userOwns(ctx, resource, id, uid) ||
      privilegeOf(ctx, resource, id, uid) == PrivilegeEnum.WRITE

  def userHasReadAccess[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
      id: Integer,
      uid: Integer
  ): Boolean =
    isPublic(ctx, resource, id) ||
      userHasWriteAccess(ctx, resource, id, uid) ||
      privilegeOf(ctx, resource, id, uid) == PrivilegeEnum.READ

  /** The owning user, or null when the resource does not exist. */
  def owner[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
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

  /**
    * The owner's email.
    *
    * @throws jakarta.ws.rs.ForbiddenException if the caller cannot read the resource.
    */
  def ownerEmail[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
      id: Integer,
      requesterUid: Integer
  ): String = {
    requireReadAccess(ctx, resource, id, requesterUid)
    Option(owner(ctx, resource, id)).map(_.getEmail).getOrElse("")
  }

  /**
    * Everyone the resource is shared with, excluding the owner's own row.
    *
    * Read access is required rather than write,
    *
    * @throws jakarta.ws.rs.ForbiddenException if the caller cannot read the resource.
    */
  def accessList[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
      id: Integer,
      requesterUid: Integer
  ): java.util.List[AccessEntry] = {
    requireReadAccess(ctx, resource, id, requesterUid)
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
    * Every resource of this type the user may see: the ones they hold an explicit grant on, plus
    * every public one, with public entries dropped when they duplicate a granted entry.
    *
    * @param pojoClass  the generated POJO the resource table maps into
    * @param idOf       reads the resource's id, used to de-duplicate the two passes
    * @param fromGrant  builds an entry the user has an explicit grant on
    * @param fromPublic builds an entry visible only because the resource is public
    */
  def listVisible[R <: Record, A <: Record, P, D](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
      uid: Integer,
      pojoClass: Class[P],
      idOf: P => Integer,
      includePublic: Boolean = true
  )(
      fromGrant: (P, String, PrivilegeEnum, Boolean) => Option[D],
      fromPublic: (P, String) => Option[D]
  ): List[D] = {
    // (id, entry) pairs so the public pass can skip ids already granted, without re-querying
    val granted: List[(Integer, D)] = ctx
      .select()
      .from(
        resource.table
          .leftJoin(resource.accessTable)
          .on(resource.accessIdField.eq(resource.idField))
          .leftJoin(USER)
          .on(USER.UID.eq(resource.ownerUidField))
      )
      .where(resource.accessUidField.eq(uid))
      .fetch()
      .asScala
      .toList
      .flatMap { record =>
        val entity = record.into(resource.table).into(pojoClass)
        val privilege = record.into(resource.accessTable).get(resource.privilegeField)
        val isOwner = record.into(resource.table).get(resource.ownerUidField) == uid
        fromGrant(entity, record.into(USER).getEmail, privilege, isOwner)
          .map(entry => (idOf(entity), entry))
      }

    val grantedIds = granted.map(_._1).toSet

    val public =
      if (!includePublic) Nil
      else
        ctx
          .select()
          .from(
            resource.table
              .leftJoin(USER)
              .on(USER.UID.eq(resource.ownerUidField))
          )
          .where(resource.isPublicField.eq(true))
          .fetch()
          .asScala
          .toList
          .flatMap { record =>
            val entity = record.into(resource.table).into(pojoClass)
            if (grantedIds.contains(idOf(entity))) None
            else fromPublic(entity, record.into(USER).getEmail)
          }

    granted.map(_._2) ++ public
  }

  /**
    * Grants `privilege` to the user with `email`, replacing any privilege they already hold.
    */
  def grant[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
      id: Integer,
      email: String,
      privilege: String,
      requesterUid: Integer
  ): Response = {
    requireWriteAccess(ctx, resource, id, requesterUid)
    val granteeUid = resolveUidByEmail(ctx, email)
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

  /**
    * Removes the user's explicit grant; a no-op when they hold none.
    *
    * @throws jakarta.ws.rs.ForbiddenException if the caller cannot modify the resource.
    * @throws jakarta.ws.rs.BadRequestException if the email does not match a registered user.
    */
  def revoke[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
      id: Integer,
      email: String,
      requesterUid: Integer
  ): Response = {
    requireWriteAccess(ctx, resource, id, requesterUid)
    val granteeUid = resolveUidByEmail(ctx, email)

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
      resource: ResourceTables[R, A],
      id: Integer,
      uid: Integer
  ): Unit =
    if (!userHasWriteAccess(ctx, resource, id, uid)) {
      throw new ForbiddenException(
        s"You do not have permission to modify ${resource.label} $id"
      )
    }

  private def requireReadAccess[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
      id: Integer,
      uid: Integer
  ): Unit =
    if (!userHasReadAccess(ctx, resource, id, uid)) {
      throw new ForbiddenException(
        s"You do not have access to ${resource.label} $id"
      )
    }

  /**
    * Resolves an email to its user id, throwing BadRequestException (400) when no registered
    * account matches — the service registers no ExceptionMapper for NullPointerException, so a
    * bare dereference surfaces as an opaque HTTP 500. Shared by grant/revoke.
    */
  private def resolveUidByEmail(ctx: DSLContext, email: String): Integer = {
    val user = new UserDao(ctx.configuration()).fetchOneByEmail(email)
    if (user == null || user.getIsPlaceholder) {
      throw new BadRequestException(s"No registered user with email $email")
    }
    user.getUid
  }

  /**
    * Emails of the owners of every resource the caller has an explicit grant on, for the
    * owner facet on list pages.
    */
  def ownerEmailsVisibleTo[R <: Record, A <: Record](
      ctx: DSLContext,
      resource: ResourceTables[R, A],
      uid: Integer
  ): java.util.List[String] =
    ctx
      .selectDistinct(USER.EMAIL)
      .from(USER)
      .join(resource.table)
      .on(resource.ownerUidField.eq(USER.UID))
      .join(resource.accessTable)
      .on(resource.accessIdField.eq(resource.idField))
      .where(resource.accessUidField.eq(uid))
      .fetchInto(classOf[String])

}
