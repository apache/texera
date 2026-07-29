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

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.apache.texera.web.resource.dashboard.user.dataset.DatasetResource.DashboardDataset
import org.apache.texera.web.resource.dashboard.{FulltextSearchQueryUtils}
import org.apache.texera.web.resource.dashboard.DatasetSearchQueryBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, DatasetDao, DatasetUserAccessDao}
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetUserAccess}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import java.time.OffsetDateTime
import java.util
import org.apache.texera.web.resource.dashboard.SearchQueryBuilder.DATASET_RESOURCE_TYPE

class DatasetResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // An example creation time to test Account Creation Time attribute
  private val exampleCreationTime: OffsetDateTime =
    OffsetDateTime.parse("2025-01-01T00:00:00Z")

  private val ownerUser: User = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("owner_user")
    user.setRole(UserRoleEnum.ADMIN)
    user.setEmail("owner_user@mail.com")
    user.setPassword("123")
    user.setComment("test_comment")
    user.setAccountCreationTime(exampleCreationTime)
    user
  }

  private val testUser: User = {
    val user = new User
    user.setUid(Integer.valueOf(2))
    user.setName("test_user")
    user.setEmail("test_user@mail.com")
    user.setRole(UserRoleEnum.REGULAR)
    user.setPassword("123")
    user.setComment("test_comment2")
    user.setAccountCreationTime(exampleCreationTime)
    user
  }

  private val testDatasetRecord: Dataset = {
    val dataset = new Dataset()
    dataset.setName("test_dataset1")
    dataset.setDescription("keyword_in_dataset_description")
    dataset.setIsPublic(true)
    dataset.setDid(Integer.valueOf(1))
    dataset
  }

  private val sessionUser1: SessionUser = {
    new SessionUser(ownerUser)
  }

  private val sessionUser2: SessionUser = {
    new SessionUser(testUser)
  }

  // get context lazily
  private lazy val datasetDao: DatasetDao = {
    new DatasetDao(getDSLContext.configuration())
  }

  private lazy val datasetUserAccessDao: DatasetUserAccessDao = {
    new DatasetUserAccessDao(getDSLContext.configuration())
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    FulltextSearchQueryUtils.usePgroonga = false // disable pgroonga
    // add test user directly
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(testUser)
  }

  override protected def beforeEach(): Unit = {
    // Clean up environment before each test case
  }

  override protected def afterEach(): Unit = {
    // 1. Delete access rows before the dataset
    val datasetUserAccessDao = new DatasetUserAccessDao(getDSLContext.configuration())
    getDSLContext
      .deleteFrom(org.apache.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS)
      .execute()
    // 2. Fetch all datasets owned by the owner
    val datasets = datasetDao.fetchByOwnerUid(ownerUser.getUid())
    if (!datasets.isEmpty) {
      datasetDao.delete(datasets)
    }
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  private def getKeywordsArray(keywords: String*): util.ArrayList[String] = {
    val keywordsList = new util.ArrayList[String]()
    for (keyword <- keywords) {
      keywordsList.add(keyword)
    }
    keywordsList
  }

  private def assertSameDataset(a: Dataset, b: DashboardDataset): Unit = {
    assert(a.getName == b.dataset.getName)
  }

  "User.accountCreationTime" should "be persisted and retrievable via UserDao" in {
    val userDao = new UserDao(getDSLContext.configuration())
    val u1 = userDao.fetchOneByUid(Integer.valueOf(1))
    val u2 = userDao.fetchOneByUid(Integer.valueOf(2))

    assert(u1.getAccountCreationTime != null)
    assert(u2.getAccountCreationTime != null)

    assert(u1.getAccountCreationTime.isEqual(exampleCreationTime))
    assert(u2.getAccountCreationTime.isEqual(exampleCreationTime))
  }

  it should "remain unchanged when updating unrelated fields" in {
    val userDao = new UserDao(getDSLContext.configuration())
    val u1 = userDao.fetchOneByUid(Integer.valueOf(1))
    val originalTime = u1.getAccountCreationTime

    u1.setComment("updated_comment")
    userDao.update(u1)

    val test_u1 = userDao.fetchOneByUid(Integer.valueOf(1))
    assert(test_u1.getAccountCreationTime.isEqual(originalTime))
  }

  "DatasetResource /owner view" should "get deduplicated datasets created by owner and shared publicly" in {
    // Only metadatas of dataset and the user is maintained - no dataset is actually created in LakeFS
    // Create dataset
    val datasetDao = new DatasetDao(getDSLContext.configuration())
    testDatasetRecord.setOwnerUid(ownerUser.getUid)
    datasetDao.insert(testDatasetRecord)

    // Give write access to the owner user
    datasetUserAccessDao.insert(
      new DatasetUserAccess(
        testDatasetRecord.getDid,
        ownerUser.getUid,
        PrivilegeEnum.WRITE
      )
    )

    // Build the query - bypasses DashboarResource
    val query =
      DatasetSearchQueryBuilder.constructQuery(
        ownerUser.getUid,
        SearchQueryParams(resourceType = DATASET_RESOURCE_TYPE),
        includePublic = true
      )
    // Assert the length of returned dataset
    val datasetEntryList = getDSLContext.fetch(query)
    assert(datasetEntryList.size() == 1)
  }

  "/search API" should "deduplicate datasets shared both publicly and explicitly with NON-WRITE permissions" in {
    // Only metadatas of dataset and the user is maintained - no dataset is actually created in LakeFS
    // Create dataset
    val datasetDao = new DatasetDao(getDSLContext.configuration())
    testDatasetRecord.setOwnerUid(ownerUser.getUid)
    datasetDao.insert(testDatasetRecord)

    // Give write access to the owner user
    datasetUserAccessDao.insert(
      new DatasetUserAccess(
        ownerUser.getUid,
        testDatasetRecord.getDid,
        PrivilegeEnum.WRITE
      )
    )

    datasetUserAccessDao.insert(
      new DatasetUserAccess(
        testDatasetRecord.getDid,
        testUser.getUid,
        PrivilegeEnum.READ
      )
    )

    // Build the query
    val query =
      DatasetSearchQueryBuilder.constructQuery(
        testUser.getUid,
        SearchQueryParams(resourceType = DATASET_RESOURCE_TYPE),
        includePublic = true
      )
    // Assert the length of returned dataset
    val datasetEntryList = getDSLContext.fetch(query)
    assert(datasetEntryList.size() == 1)
  }
}
