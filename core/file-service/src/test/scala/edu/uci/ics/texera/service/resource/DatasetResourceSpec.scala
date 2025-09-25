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

package edu.uci.ics.texera.service.resource

import edu.uci.ics.amber.core.storage.util.LakeFSStorageClient
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{DatasetDao, UserDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Dataset, User}
import edu.uci.ics.texera.service.MockLakeFS
import jakarta.ws.rs.{BadRequestException, ForbiddenException}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatasetResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll {

  private val testUser: User = {
    val user = new User
    user.setName("test_user")
    user.setPassword("123")
    user.setEmail("test_user@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  private val testUser2: User = {
    val user = new User
    user.setName("test_user2")
    user.setPassword("123")
    user.setEmail("test_user2@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  private val testDataset: Dataset = {
    val dataset = new Dataset
    dataset.setName("test-dataset")
    dataset.setRepositoryName("test-dataset")
    dataset.setIsPublic(true)
    dataset.setIsDownloadable(true)
    dataset.setDescription("dataset for test")
    dataset
  }

  lazy val datasetDao = new DatasetDao(getDSLContext.configuration())

  lazy val datasetResource = new DatasetResource()

  lazy val sessionUser = new SessionUser(testUser)
  lazy val sessionUser2 = new SessionUser(testUser2)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // init db
    initializeDBAndReplaceDSLContext()

    // insert test user
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(testUser)
    userDao.insert(testUser2)

    // insert test dataset
    testDataset.setOwnerUid(testUser.getUid)
    datasetDao.insert(testDataset)
  }

  "createDataset" should "create dataset successfully if user does not have a dataset with the same name" in {
    val createDatasetRequest = DatasetResource.CreateDatasetRequest(
      datasetName = "new-dataset",
      datasetDescription = "description for new dataset",
      isDatasetPublic = false,
      isDatasetDownloadable = true
    )

    val createdDataset = datasetResource.createDataset(createDatasetRequest, sessionUser)
    createdDataset.dataset.getName shouldEqual "new-dataset"
    createdDataset.dataset.getDescription shouldEqual "description for new dataset"
    createdDataset.dataset.getIsPublic shouldBe false
    createdDataset.dataset.getIsDownloadable shouldBe true
  }

  it should "refuse to create dataset if user already has a dataset with the same name" in {
    val createDatasetRequest = DatasetResource.CreateDatasetRequest(
      datasetName = "test-dataset",
      datasetDescription = "description for new dataset",
      isDatasetPublic = false,
      isDatasetDownloadable = true
    )

    assertThrows[BadRequestException] {
      datasetResource.createDataset(createDatasetRequest, sessionUser)
    }
  }

  it should "create dataset successfully if another user has a dataset with the same name" in {
    val createDatasetRequest = DatasetResource.CreateDatasetRequest(
      datasetName = "test-dataset",
      datasetDescription = "description for new dataset",
      isDatasetPublic = false,
      isDatasetDownloadable = true
    )

    val createdDataset = datasetResource.createDataset(createDatasetRequest, sessionUser2)
    createdDataset.dataset.getName shouldEqual "test-dataset"
    createdDataset.dataset.getDescription shouldEqual "description for new dataset"
    createdDataset.dataset.getIsPublic shouldBe false
    createdDataset.dataset.getIsDownloadable shouldBe true
  }

  it should "delete dataset successfully if user owns it" in {
    // insert a dataset directly into DB
    val dataset = new Dataset
    dataset.setName("delete-ds")
    dataset.setRepositoryName("delete-ds")
    dataset.setDescription("for delete test")
    dataset.setOwnerUid(testUser.getUid)
    dataset.setIsPublic(true)
    dataset.setIsDownloadable(true)
    datasetDao.insert(dataset)

    // create repo in LakeFS to match dataset
    LakeFSStorageClient.initRepo(dataset.getRepositoryName)

    // delete via DatasetResource
    val response = datasetResource.deleteDataset(dataset.getDid, sessionUser)

    // assert: response OK and DB no longer contains dataset
    response.getStatus shouldEqual 200
    datasetDao.fetchOneByDid(dataset.getDid) shouldBe null
  }

  it should "refuse to delete dataset if not owned by user" in {
    // insert a dataset directly into DB
    val dataset = new Dataset
    dataset.setName("user1-ds")
    dataset.setRepositoryName("user1-ds")
    dataset.setDescription("for forbidden test")
    dataset.setOwnerUid(testUser.getUid)
    dataset.setIsPublic(true)
    dataset.setIsDownloadable(true)
    datasetDao.insert(dataset)

    // create repo in LakeFS to match dataset
    LakeFSStorageClient.initRepo(dataset.getRepositoryName)

    // user2 tries to delete, should throw ForbiddenException
    assertThrows[ForbiddenException] {
      datasetResource.deleteDataset(dataset.getDid, sessionUser2)
    }

    // dataset still exists in DB
    datasetDao.fetchOneByDid(dataset.getDid) should not be null
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }
}
