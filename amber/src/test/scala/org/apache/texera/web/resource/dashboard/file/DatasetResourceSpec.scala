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

package org.apache.texera.web.resource.dashboard.file

import org.apache.texera.dao.DatabaseFlatSpec
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.apache.texera.web.resource.dashboard.DatasetSearchQueryBuilder
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, DatasetDao, DatasetUserAccessDao}
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetUserAccess}
import java.time.OffsetDateTime
import org.apache.texera.web.resource.dashboard.SearchQueryBuilder.DATASET_RESOURCE_TYPE

class DatasetResourceSpec
    extends DatabaseFlatSpec(cleanupBeforeEach = false, cleanupAfterEach = true) {

  // An example creation time to test Account Creation Time attribute
  private val exampleCreationTime: OffsetDateTime =
    OffsetDateTime.parse("2025-01-01T00:00:00Z")

  private def seededUser(uid: Int, name: String, role: UserRoleEnum, comment: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setRole(role)
    user.setEmail(s"$name@mail.com")
    user.setComment(comment)
    user.setAccountCreationTime(exampleCreationTime)
    user
  }

  private val ownerUser = seededUser(1, "owner_user", UserRoleEnum.ADMIN, "test_comment")
  private val testUser = seededUser(2, "test_user", UserRoleEnum.REGULAR, "test_comment2")

  private val testDatasetRecord: Dataset = {
    val dataset = new Dataset()
    dataset.setName("test_dataset1")
    dataset.setDescription("keyword_in_dataset_description")
    dataset.setIsPublic(true)
    dataset.setDid(Integer.valueOf(1))
    dataset
  }

  private lazy val userDao = new UserDao(getDSLContext.configuration())
  private lazy val datasetDao = new DatasetDao(getDSLContext.configuration())
  private lazy val datasetUserAccessDao = new DatasetUserAccessDao(getDSLContext.configuration())

  override protected def onBeforeAll(): Unit = {
    userDao.insert(ownerUser)
    userDao.insert(testUser)
  }

  override protected def cleanup(): Unit = {
    // 1. Delete access rows before the dataset
    getDSLContext
      .deleteFrom(org.apache.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS)
      .execute()
    // 2. Fetch all datasets owned by the owner
    val datasets = datasetDao.fetchByOwnerUid(ownerUser.getUid())
    if (!datasets.isEmpty) {
      datasetDao.delete(datasets)
    }
  }

  "User.accountCreationTime" should "be persisted and retrievable via UserDao" in {
    val u1 = userDao.fetchOneByUid(Integer.valueOf(1))
    val u2 = userDao.fetchOneByUid(Integer.valueOf(2))

    assert(u1.getAccountCreationTime != null)
    assert(u2.getAccountCreationTime != null)

    assert(u1.getAccountCreationTime.isEqual(exampleCreationTime))
    assert(u2.getAccountCreationTime.isEqual(exampleCreationTime))
  }

  it should "remain unchanged when updating unrelated fields" in {
    val u1 = userDao.fetchOneByUid(Integer.valueOf(1))
    val originalTime = u1.getAccountCreationTime

    u1.setComment("updated_comment")
    userDao.update(u1)

    val test_u1 = userDao.fetchOneByUid(Integer.valueOf(1))
    assert(test_u1.getAccountCreationTime.isEqual(originalTime))
  }

  private def grant(uid: Integer, privilege: PrivilegeEnum): Unit =
    datasetUserAccessDao.insert(new DatasetUserAccess(testDatasetRecord.getDid, uid, privilege))

  // These cases query metadata directly. A LakeFS repository is needed only for hydration.
  private def seedPublicDataset(): Unit = {
    testDatasetRecord.setOwnerUid(ownerUser.getUid)
    datasetDao.insert(testDatasetRecord)
    grant(ownerUser.getUid, PrivilegeEnum.WRITE)
  }

  private def assertSingleDataset(uid: Integer): Unit = {
    val query = DatasetSearchQueryBuilder.constructQuery(
      uid,
      SearchQueryParams(resourceType = DATASET_RESOURCE_TYPE),
      includePublic = true
    )
    val datasetEntryList = getDSLContext.fetch(query)
    assert(datasetEntryList.size() == 1)
  }

  "DatasetResource /owner view" should "get deduplicated datasets created by owner and shared publicly" in {
    seedPublicDataset()
    assertSingleDataset(ownerUser.getUid)
  }

  "/search API" should "deduplicate datasets shared both publicly and explicitly with NON-WRITE permissions" in {
    seedPublicDataset()
    grant(testUser.getUid, PrivilegeEnum.READ)
    assertSingleDataset(testUser.getUid)
  }
}
