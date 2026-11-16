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

import jakarta.ws.rs.core.Response
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.ModelDao
import org.apache.texera.dao.jooq.generated.tables.pojos.{Model, User}
import org.jooq.DSLContext

class ModelAccessResourceSpec extends AccessResourceBehaviors(ResourceTables.Model) {

  private lazy val accessResource = new ModelAccessResource()

  override protected def insertResource(
      name: String,
      description: String,
      isPublic: Boolean,
      ownerUid: Integer
  ): Integer = {
    val model = new Model
    model.setName(name)
    model.setRepositoryName(name)
    model.setIsPublic(isPublic)
    model.setIsDownloadable(true)
    model.setDescription(description)
    model.setFramework("pytorch")
    model.setOwnerUid(ownerUid)
    new ModelDao(getDSLContext.configuration()).insert(model)
    model.getMid
  }

  override protected def resourceIsPublic(ctx: DSLContext, id: Integer): Boolean =
    ModelAccessResource.isModelPublic(ctx, id)

  override protected def userOwnsResource(ctx: DSLContext, id: Integer, uid: Integer): Boolean =
    ModelAccessResource.userOwnModel(ctx, id, uid)

  override protected def userPrivilege(ctx: DSLContext, id: Integer, uid: Integer): PrivilegeEnum =
    ModelAccessResource.getModelUserAccessPrivilege(ctx, id, uid)

  override protected def userCanRead(ctx: DSLContext, id: Integer, uid: Integer): Boolean =
    ModelAccessResource.userHasReadAccess(ctx, id, uid)

  override protected def userCanWrite(ctx: DSLContext, id: Integer, uid: Integer): Boolean =
    ModelAccessResource.userHasWriteAccess(ctx, id, uid)

  override protected def resourceOwner(ctx: DSLContext, id: Integer): User =
    ModelAccessResource.getOwner(ctx, id)

  override protected def grant(
      id: Integer,
      email: String,
      privilege: String,
      user: SessionUser
  ): Response =
    accessResource.grantAccess(id, email, privilege, user)

  override protected def revoke(id: Integer, email: String, user: SessionUser): Response =
    accessResource.revokeAccess(id, email, user)

  override protected def accessListOf(
      id: Integer,
      user: SessionUser
  ): java.util.List[ResourceAccess.AccessEntry] =
    accessResource.getAccessList(id, user)

  override protected def ownerEmail(id: Integer, user: SessionUser): String =
    accessResource.getOwnerEmailOfModel(id, user)
}
