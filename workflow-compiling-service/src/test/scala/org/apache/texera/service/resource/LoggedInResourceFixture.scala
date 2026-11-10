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

import io.dropwizard.auth.AuthValueFactoryProvider
import io.dropwizard.testing.junit5.ResourceExtension
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.auth.{JwtAuth, JwtAuthFilter, SessionUser, UnauthorizedExceptionMapper}
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, DatasetVersionDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetVersion, User}
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.scalatest.TestSuite

/**
  * What a resource spec of this service needs to make requests as a logged-in user: the
  * providers production registers through `AuthFeatures`, and two users around one private
  * dataset, owned by `owner` and shared with nobody.
  */
trait LoggedInResourceFixture extends MockTexeraDB { this: TestSuite =>

  private def userNamed(uid: Int, name: String): User = {
    val user = new User
    user.setUid(Integer.valueOf(uid))
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  protected val owner: User = userNamed(1, "owner")
  protected val otherUser: User = userNamed(2, "other_user")

  protected val privateDatasetRepositoryName = "private-dataset-repository"
  protected val privateDatasetVersionHash = "5f0c7a1be2d94c3fa6b87d10e4c9a2b35d6e7f80"

  /** A file of the private dataset, named the way a user names it in an operator. */
  protected val privateDatasetFilePath = "/dataset/owner@test.com/private_dataset/v1/notes.txt"

  protected def resourcesFor(resource: AnyRef): ResourceExtension =
    ResourceExtension
      .builder()
      .setMapper(objectMapper)
      .addProvider(classOf[JwtAuthFilter])
      .addProvider(classOf[UnauthorizedExceptionMapper])
      .addProvider(classOf[RolesAllowedDynamicFeature])
      .addProvider(new AuthValueFactoryProvider.Binder(classOf[SessionUser]))
      .addResource(resource)
      .build()

  protected def bearerTokenOf(user: User): String =
    s"Bearer ${JwtAuth.jwtToken(JwtAuth.jwtClaims(user))}"

  protected def insertUsersAndPrivateDataset(): Unit = {
    val configuration = getDSLContext.configuration()
    val userDao = new UserDao(configuration)
    userDao.insert(owner)
    userDao.insert(otherUser)

    val dataset = new Dataset
    dataset.setDid(Integer.valueOf(1))
    dataset.setName("private_dataset")
    dataset.setRepositoryName(privateDatasetRepositoryName)
    dataset.setDescription("private dataset for test")
    dataset.setIsPublic(false)
    dataset.setOwnerUid(owner.getUid)
    new DatasetDao(configuration).insert(dataset)

    val version = new DatasetVersion
    version.setDid(dataset.getDid)
    version.setDvid(Integer.valueOf(1))
    version.setName("v1")
    version.setCreatorUid(owner.getUid)
    version.setVersionHash(privateDatasetVersionHash)
    new DatasetVersionDao(configuration).insert(version)
  }
}
