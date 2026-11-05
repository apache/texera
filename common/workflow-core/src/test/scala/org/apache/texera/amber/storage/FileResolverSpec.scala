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

package org.apache.texera.amber.storage

import org.apache.texera.amber.core.storage.FileResolver
import org.apache.commons.vfs2.FileNotFoundException
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  DatasetDao,
  DatasetUserAccessDao,
  DatasetVersionDao,
  UserDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  DatasetUserAccess,
  DatasetVersion,
  User
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.nio.file.Paths

class FileResolverSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testUser: User = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("test_user")
    user.setRole(UserRoleEnum.ADMIN)
    user.setPassword("123")
    user.setEmail("test_user@test.com")
    user
  }

  private val testDataset: Dataset = {
    val dataset = new Dataset
    dataset.setDid(Integer.valueOf(1))
    dataset.setName("test_dataset")
    dataset.setRepositoryName("test_dataset")
    dataset.setDescription("dataset for test")
    dataset.setIsPublic(true)
    dataset.setOwnerUid(Integer.valueOf(1))
    dataset
  }

  private val testDatasetVersion1: DatasetVersion = {
    val datasetVersion = new DatasetVersion
    datasetVersion.setDid(Integer.valueOf(1))
    datasetVersion.setName("v1")
    datasetVersion.setDvid(Integer.valueOf(1))
    datasetVersion.setCreatorUid(Integer.valueOf(1))
    datasetVersion.setVersionHash("97fd4c2a755b69b7c66d322eab40b7e5c2ad5d10")
    datasetVersion
  }

  private val testDatasetVersion2: DatasetVersion = {
    val datasetVersion = new DatasetVersion
    datasetVersion.setDid(Integer.valueOf(1))
    datasetVersion.setName("v2")
    datasetVersion.setDvid(Integer.valueOf(2))
    datasetVersion.setCreatorUid(Integer.valueOf(1))
    datasetVersion.setVersionHash("37966c92cb3a8bee1f9d8e21937aa8faa5e48513")
    datasetVersion
  }

  private def userNamed(uid: Int, name: String): User = {
    val user = new User
    user.setUid(Integer.valueOf(uid))
    user.setName(name)
    user.setRole(UserRoleEnum.REGULAR)
    user.setPassword("123")
    user.setEmail(s"$name@test.com")
    user
  }

  private val grantedUser: User = userNamed(2, "granted_user")
  private val otherUser: User = userNamed(3, "other_user")

  private val privateDataset: Dataset = {
    val dataset = new Dataset
    dataset.setDid(Integer.valueOf(2))
    dataset.setName("private_dataset")
    dataset.setRepositoryName("private_dataset_repo")
    dataset.setDescription("private dataset for test")
    dataset.setIsPublic(false)
    dataset.setOwnerUid(testUser.getUid)
    dataset
  }

  private val privateDatasetVersion: DatasetVersion = {
    val datasetVersion = new DatasetVersion
    datasetVersion.setDid(privateDataset.getDid)
    datasetVersion.setName("v1")
    datasetVersion.setDvid(Integer.valueOf(3))
    datasetVersion.setCreatorUid(testUser.getUid)
    datasetVersion.setVersionHash("5f0c7a1be2d94c3fa6b87d10e4c9a2b35d6e7f80")
    datasetVersion
  }

  private val privateDatasetFilePath = "/test_user@test.com/private_dataset/v1/data.csv"
  private val privateDatasetFileUri =
    s"${FileResolver.DATASET_FILE_URI_SCHEME}:///${privateDataset.getRepositoryName}/${privateDatasetVersion.getVersionHash}/data.csv"

  // names nothing: the owner exists, the dataset does not
  private val missingDatasetFilePath = "/test_user@test.com/no_such_dataset/v1/data.csv"

  private val localCsvFilePath = "common/workflow-core/src/test/resources/country_sales_small.csv"

  private val datasetACsvFilePath = "/test_user@test.com/test_dataset/v2/directory/a.csv"

  private val dataset1TxtFilePath = "/test_user@test.com/test_dataset/v1/1.txt"

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()

    // add test user
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(testUser)

    // add test dataset
    val datasetDao = new DatasetDao(getDSLContext.configuration())
    datasetDao.insert(testDataset)

    // add test dataset versions
    val datasetVersionDao = new DatasetVersionDao(getDSLContext.configuration())
    datasetVersionDao.insert(testDatasetVersion1)
    datasetVersionDao.insert(testDatasetVersion2)

    // add the private dataset, and the two users who do not own it
    userDao.insert(grantedUser)
    userDao.insert(otherUser)
    datasetDao.insert(privateDataset)
    datasetVersionDao.insert(privateDatasetVersion)

    val datasetGrant = new DatasetUserAccess
    datasetGrant.setDid(privateDataset.getDid)
    datasetGrant.setUid(grantedUser.getUid)
    datasetGrant.setPrivilege(PrivilegeEnum.READ)
    new DatasetUserAccessDao(getDSLContext.configuration()).insert(datasetGrant)
  }

  "resolveVersionedResource" should "resolve dataset file correctly" in {
    val datasetACsvUri = FileResolver.resolveVersionedResource(datasetACsvFilePath)
    val dataset1TxtUri = FileResolver.resolveVersionedResource(dataset1TxtFilePath)

    assert(
      datasetACsvUri.toString == f"${FileResolver.DATASET_FILE_URI_SCHEME}:///${testDataset.getRepositoryName}/${testDatasetVersion2.getVersionHash}/directory/a.csv"
    )
    assert(
      dataset1TxtUri.toString == f"${FileResolver.DATASET_FILE_URI_SCHEME}:///${testDataset.getRepositoryName}/${testDatasetVersion1.getVersionHash}/1.txt"
    )
  }

  it should "resolve a private dataset without a user scope" in {
    assert(
      FileResolver
        .resolveVersionedResource(privateDatasetFilePath)
        .toString == privateDatasetFileUri
    )
  }

  it should "throw not found exception" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource("some/random/path")
    }
  }

  it should "not resolve a file on the local disk, even one that exists" in {
    assert(Paths.get(localCsvFilePath).toFile.exists())
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource(localCsvFilePath)
    }
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource(Paths.get(localCsvFilePath).toAbsolutePath.toString)
    }
  }

  it should "not accept a name that is already a URI" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource(privateDatasetFileUri)
    }
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource(Paths.get(localCsvFilePath).toUri.toString)
    }
  }

  "resolveForUser" should "resolve a private dataset file for its owner" in {
    assert(
      FileResolver.resolveForUser(privateDatasetFilePath, testUser.getUid).toString
        == privateDatasetFileUri
    )
  }

  it should "resolve a private dataset file for a user who was granted read access" in {
    assert(
      FileResolver.resolveForUser(privateDatasetFilePath, grantedUser.getUid).toString
        == privateDatasetFileUri
    )
  }

  it should "not resolve a private dataset file for any other user" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveForUser(privateDatasetFilePath, otherUser.getUid)
    }
  }

  it should "resolve a public dataset file for a user with no grant on it" in {
    assert(
      FileResolver.resolveForUser(dataset1TxtFilePath, otherUser.getUid).toString ==
        f"${FileResolver.DATASET_FILE_URI_SCHEME}:///${testDataset.getRepositoryName}/${testDatasetVersion1.getVersionHash}/1.txt"
    )
  }

  it should "throw FileNotFoundException with the input path when the dataset is not readable or missing" in {
    val unreadable = intercept[FileNotFoundException] {
      FileResolver.resolveForUser(privateDatasetFilePath, otherUser.getUid)
    }
    val missing = intercept[FileNotFoundException] {
      FileResolver.resolveForUser(missingDatasetFilePath, otherUser.getUid)
    }
    assert(
      unreadable.getMessage.replace(privateDatasetFilePath, "<path>")
        == missing.getMessage.replace(missingDatasetFilePath, "<path>")
    )
    assert(unreadable.getCause == null && missing.getCause == null)
  }

  it should "not resolve a file on the local disk" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveForUser(localCsvFilePath, testUser.getUid)
    }
    assertThrows[FileNotFoundException] {
      FileResolver.resolveForUser(
        Paths.get(localCsvFilePath).toAbsolutePath.toString,
        testUser.getUid
      )
    }
  }

  it should "not accept a name that is already a URI" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveForUser(privateDatasetFileUri, testUser.getUid)
    }
    assertThrows[FileNotFoundException] {
      FileResolver.resolveForUser(Paths.get(localCsvFilePath).toUri.toString, testUser.getUid)
    }
  }

  "forUser" should "resolve names on behalf of the user it was built for" in {
    assert(
      FileResolver.forUser(grantedUser.getUid).resolve(privateDatasetFilePath).toString
        == privateDatasetFileUri
    )
    assertThrows[FileNotFoundException] {
      FileResolver.forUser(otherUser.getUid).resolve(privateDatasetFilePath)
    }
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

}
