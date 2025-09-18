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

import edu.uci.ics.amber.config.StorageConfig
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{DatasetDao, UserDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Dataset, User}
import edu.uci.ics.texera.service.MockLakeFS
import jakarta.ws.rs.BadRequestException
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
    val datasetDao = new DatasetDao(getDSLContext.configuration())
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

  override protected def afterAll(): Unit = {
    shutdownDB()
  }
}
