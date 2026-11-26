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
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{ModelUserAccessDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{ModelUserAccess, User}
import org.apache.texera.service.MockLakeFS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.io.ByteArrayInputStream

/**
  * The fixture the [[ModelResource]] endpoint specs share.
  *
  * A spec gets the test database and LakeFS, an owner inserted ahead of the spec's
  * [[extraUsers]], and helpers that create models, upload files and grant access. The helpers
  * act as the owner unless a caller passes another session.
  *
  * @param ownerUserName the name of the user who owns the spec's models
  */
abstract class ModelResourceTestSupport(ownerUserName: String)
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll
    with ResourceTestHelpers {

  /** A user named `name`, with email `<name>@test.com` and the given role. */
  protected def mkUser(name: String, role: UserRoleEnum = UserRoleEnum.ADMIN): User = {
    val user = new User
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user.setRole(role)
    user
  }

  protected val ownerUser: User = mkUser(ownerUserName)

  /** The spec's other users, inserted after the owner, in this order. */
  protected def extraUsers: Seq[User]

  lazy val modelResource = new ModelResource()
  lazy val sessionUser = new SessionUser(ownerUser)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    (ownerUser +: extraUsers).foreach(userDao.insert)
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  /** Creates a model through the createModel endpoint as `user` and returns it. */
  protected def createModelFixture(
      name: String,
      description: String,
      isPublic: Boolean = false,
      isDownloadable: Boolean = true,
      framework: String = "pytorch",
      format: String = null,
      user: SessionUser = sessionUser
  ): ModelResource.DashboardModel =
    modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = name,
        modelDescription = description,
        isModelPublic = isPublic,
        isModelDownloadable = isDownloadable,
        framework = framework,
        format = format
      ),
      user
    )

  /** Uploads `bytes` as file `path` of model `mid` in one request as `user`. */
  protected def uploadOneShot(
      mid: Integer,
      path: String,
      bytes: Array[Byte],
      user: SessionUser = sessionUser
  ): Response =
    modelResource.uploadOneFileToModel(
      mid,
      urlEnc(path),
      "upload",
      new ByteArrayInputStream(bytes),
      mkHeaders(bytes.length.toLong),
      user
    )

  /** Uploads `bytes` as file `path` of model `mid` as the owner and asserts a 200. */
  protected def upload(mid: Integer, path: String, bytes: Array[Byte]): Unit =
    uploadOneShot(mid, path, bytes).getStatus shouldEqual 200

  /** Grants user `uid` `privilege` on model `mid` by inserting the access row directly. */
  protected def grantAccess(mid: Integer, uid: Integer, privilege: PrivilegeEnum): Unit =
    new ModelUserAccessDao(getDSLContext.configuration())
      .insert(new ModelUserAccess(mid, uid, privilege))
}
