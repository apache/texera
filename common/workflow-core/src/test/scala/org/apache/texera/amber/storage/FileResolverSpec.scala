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
  ModelDao,
  ModelUserAccessDao,
  ModelVersionDao,
  UserDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  DatasetUserAccess,
  DatasetVersion,
  Model,
  ModelUserAccess,
  ModelVersion,
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

  private val testModel: Model = {
    val model = new Model
    model.setMid(Integer.valueOf(1))
    model.setName("test_model")
    model.setRepositoryName("model-1")
    model.setDescription("model for test")
    model.setIsPublic(true)
    model.setIsDownloadable(true)
    model.setOwnerUid(Integer.valueOf(1))
    model
  }

  private val testModelVersion1: ModelVersion = {
    val modelVersion = new ModelVersion
    modelVersion.setMid(Integer.valueOf(1))
    modelVersion.setName("v1")
    modelVersion.setMvid(Integer.valueOf(1))
    modelVersion.setCreatorUid(Integer.valueOf(1))
    modelVersion.setVersionHash("a1b2c3d4e5f60718293a4b5c6d7e8f9001122334")
    modelVersion
  }

  private val testModelVersion2: ModelVersion = {
    val modelVersion = new ModelVersion
    modelVersion.setMid(Integer.valueOf(1))
    modelVersion.setName("v2")
    modelVersion.setMvid(Integer.valueOf(2))
    modelVersion.setCreatorUid(Integer.valueOf(1))
    modelVersion.setVersionHash("998877665544332211ffeeddccbbaa0099887766")
    modelVersion
  }

  // Three users around one private dataset and one private model, both owned by `testUser`:
  // `grantedUser` holds a READ grant on each, `otherUser` holds nothing.
  private def userNamed(uid: Int, name: String): User = {
    val user = new User
    user.setUid(Integer.valueOf(uid))
    user.setName(name)
    user.setRole(UserRoleEnum.REGULAR)
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

  private val privateModel: Model = {
    val model = new Model
    model.setMid(Integer.valueOf(2))
    model.setName("private_model")
    model.setRepositoryName("model-2")
    model.setDescription("private model for test")
    model.setIsPublic(false)
    model.setIsDownloadable(true)
    model.setOwnerUid(testUser.getUid)
    model
  }

  private val privateModelVersion: ModelVersion = {
    val modelVersion = new ModelVersion
    modelVersion.setMid(privateModel.getMid)
    modelVersion.setName("v1")
    modelVersion.setMvid(Integer.valueOf(3))
    modelVersion.setCreatorUid(testUser.getUid)
    modelVersion.setVersionHash("0d1e2f3a4b5c6d7e8f90a1b2c3d4e5f607182930")
    modelVersion
  }

  private val privateDatasetFilePath = "/dataset/test_user@test.com/private_dataset/v1/data.csv"
  private val unprefixedPrivateDatasetFilePath = "/test_user@test.com/private_dataset/v1/data.csv"
  private val privateDatasetFileUri =
    s"${FileResolver.DATASET_FILE_URI_SCHEME}:///${privateDataset.getRepositoryName}/${privateDatasetVersion.getVersionHash}/data.csv"

  private val privateModelFilePath = "/model/test_user@test.com/private_model/v1/weights.pt"
  private val privateModelFileUri =
    s"${FileResolver.MODEL_FILE_URI_SCHEME}:///${privateModel.getRepositoryName}/${privateModelVersion.getVersionHash}/weights.pt"

  // names nothing: the owner exists, the dataset does not
  private val missingDatasetFilePath = "/dataset/test_user@test.com/no_such_dataset/v1/data.csv"

  private val localCsvFilePath = "common/workflow-core/src/test/resources/country_sales_small.csv"

  private val datasetACsvFilePath = "/dataset/test_user@test.com/test_dataset/v2/directory/a.csv"

  private val dataset1TxtFilePath = "/dataset/test_user@test.com/test_dataset/v1/1.txt"

  private val modelWeightsFilePath = "/model/test_user@test.com/test_model/v2/weights/model.pt"

  private val modelReadmeFilePath = "/model/test_user@test.com/test_model/v1/README.md"

  // Legacy unprefixed form — still resolvable as a dataset (backward compat).
  private val unprefixedDataset1TxtFilePath = "/test_user@test.com/test_dataset/v1/1.txt"

  // Leading segment names no known dataset owner, so this is not resolvable.
  private val unknownResourceTypeFilePath =
    "/notAResourceType/test_user@test.com/test_dataset/v1/1.txt"

  // A model name presented under the dataset prefix must not resolve as a dataset.
  private val modelNameUnderDatasetPrefix = "/dataset/test_user@test.com/test_model/v1/README.md"

  // A dataset name presented under the model prefix must not resolve as a model.
  private val datasetNameUnderModelPrefix = "/model/test_user@test.com/test_dataset/v1/1.txt"

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

    // add test model
    val modelDao = new ModelDao(getDSLContext.configuration())
    modelDao.insert(testModel)

    // add test model versions
    val modelVersionDao = new ModelVersionDao(getDSLContext.configuration())
    modelVersionDao.insert(testModelVersion1)
    modelVersionDao.insert(testModelVersion2)

    // add the private dataset and model, and the two users who do not own them
    userDao.insert(grantedUser)
    userDao.insert(otherUser)
    datasetDao.insert(privateDataset)
    datasetVersionDao.insert(privateDatasetVersion)
    modelDao.insert(privateModel)
    modelVersionDao.insert(privateModelVersion)

    val datasetGrant = new DatasetUserAccess
    datasetGrant.setDid(privateDataset.getDid)
    datasetGrant.setUid(grantedUser.getUid)
    datasetGrant.setPrivilege(PrivilegeEnum.READ)
    new DatasetUserAccessDao(getDSLContext.configuration()).insert(datasetGrant)

    val modelGrant = new ModelUserAccess
    modelGrant.setMid(privateModel.getMid)
    modelGrant.setUid(grantedUser.getUid)
    modelGrant.setPrivilege(PrivilegeEnum.READ)
    new ModelUserAccessDao(getDSLContext.configuration()).insert(modelGrant)
  }

  // ---------------------------------------------------------------------------
  // resolveVersionedResource
  // ---------------------------------------------------------------------------

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

  it should "resolve model file correctly" in {
    val modelWeightsUri = FileResolver.resolveVersionedResource(modelWeightsFilePath)
    val modelReadmeUri = FileResolver.resolveVersionedResource(modelReadmeFilePath)

    assert(
      modelWeightsUri.toString == f"${FileResolver.MODEL_FILE_URI_SCHEME}:///${testModel.getRepositoryName}/${testModelVersion2.getVersionHash}/weights/model.pt"
    )
    assert(
      modelReadmeUri.toString == f"${FileResolver.MODEL_FILE_URI_SCHEME}:///${testModel.getRepositoryName}/${testModelVersion1.getVersionHash}/README.md"
    )
  }

  it should "still resolve a legacy unprefixed dataset path" in {
    val dataset1TxtUri = FileResolver.resolveVersionedResource(unprefixedDataset1TxtFilePath)

    assert(
      dataset1TxtUri.toString == f"${FileResolver.DATASET_FILE_URI_SCHEME}:///${testDataset.getRepositoryName}/${testDatasetVersion1.getVersionHash}/1.txt"
    )
  }

  it should "resolve a private dataset or model without a user scope" in {
    assert(
      FileResolver
        .resolveVersionedResource(privateDatasetFilePath)
        .toString == privateDatasetFileUri
    )
    assert(
      FileResolver.resolveVersionedResource(privateModelFilePath).toString == privateModelFileUri
    )
  }

  it should "not resolve a path whose leading segment names no dataset" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource(unknownResourceTypeFilePath)
    }
  }

  it should "keep datasets and models isolated by prefix" in {
    // a real dataset name under the models prefix is not a model, and vice-versa
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource(datasetNameUnderModelPrefix)
    }
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource(modelNameUnderDatasetPrefix)
    }
  }

  it should "throw not found exception when a prefixed path has too few segments" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource("/dataset/test_user@test.com/test_dataset")
    }
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

  it should "not pass a missing Windows path through as a URI" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveVersionedResource("C:/x/y.txt")
    }
    assertThrows[FileNotFoundException] {
      FileResolver.resolveForUser("C:/x/y.txt", testUser.getUid)
    }
  }

  // ---------------------------------------------------------------------------
  // resolveForUser
  // ---------------------------------------------------------------------------

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

  it should "apply the same rule to a legacy unprefixed dataset path" in {
    assert(
      FileResolver.resolveForUser(unprefixedPrivateDatasetFilePath, testUser.getUid).toString
        == privateDatasetFileUri
    )
    assertThrows[FileNotFoundException] {
      FileResolver.resolveForUser(unprefixedPrivateDatasetFilePath, otherUser.getUid)
    }
  }

  it should "resolve a private model file for its owner and for a user who was granted read access" in {
    assert(
      FileResolver.resolveForUser(privateModelFilePath, testUser.getUid).toString
        == privateModelFileUri
    )
    assert(
      FileResolver.resolveForUser(privateModelFilePath, grantedUser.getUid).toString
        == privateModelFileUri
    )
  }

  it should "not resolve a private model file for any other user" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveForUser(privateModelFilePath, otherUser.getUid)
    }
  }

  it should "resolve a public model file for a user with no grant on it" in {
    assert(
      FileResolver.resolveForUser(modelReadmeFilePath, otherUser.getUid).toString ==
        f"${FileResolver.MODEL_FILE_URI_SCHEME}:///${testModel.getRepositoryName}/${testModelVersion1.getVersionHash}/README.md"
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

  "isFileResolved" should "return true when the path has a multi-letter scheme" in {
    assert(FileResolver.isFileResolved("s3://bucket/key"))
    assert(
      FileResolver.isFileResolved(s"${FileResolver.DATASET_FILE_URI_SCHEME}:///repo/hash/file.csv")
    )
    assert(FileResolver.isFileResolved("file:///C:/x/y.txt"))
    assert(FileResolver.isFileResolved("file:/tmp/y.txt"))
    assert(FileResolver.isFileResolved("vfs:///repo/hash/file.csv"))
    assert(FileResolver.isFileResolved("https://example.com/y.txt"))
  }

  it should "return false when the path has no scheme" in {
    assert(!FileResolver.isFileResolved("some/random/path"))
    assert(!FileResolver.isFileResolved("/test_user@test.com/test_dataset/v1/1.txt"))
  }

  it should "return false for an absolute Windows path" in {
    // `java.net.URI` parses the drive letter as a one-letter scheme
    assert(!FileResolver.isFileResolved("C:/x/y.txt"))
    assert(!FileResolver.isFileResolved("c:/x/y.txt"))
    // a backslash is illegal in a URI, so this form never parsed as one
    assert(!FileResolver.isFileResolved("C:\\x\\y.txt"))
  }

  it should "return false for a malformed URI" in {
    // a space is illegal in a URI and makes the java.net.URI constructor throw
    assert(!FileResolver.isFileResolved("has a space"))
  }

  "parseDatasetOwnerAndName" should "extract owner email and dataset name from a valid path" in {
    assert(
      FileResolver.parseDatasetOwnerAndName("/dataset/test_user@test.com/test_dataset/v1/1.txt")
        == Some(("test_user@test.com", "test_dataset"))
    )
    // extra segments beyond the file-relative path are ignored
    assert(
      FileResolver.parseDatasetOwnerAndName("/dataset/owner@x.com/ds/v2/directory/nested/a.csv")
        == Some(("owner@x.com", "ds"))
    )
  }

  it should "still extract owner and name from a legacy unprefixed path" in {
    assert(
      FileResolver.parseDatasetOwnerAndName(unprefixedDataset1TxtFilePath)
        == Some(("test_user@test.com", "test_dataset"))
    )
  }

  it should "return None when the prefixed path has too few segments" in {
    assert(FileResolver.parseDatasetOwnerAndName("/dataset/owner@x.com/ds").isEmpty)
    assert(FileResolver.parseDatasetOwnerAndName("owner/dataset").isEmpty)
  }

  it should "return None for a null path" in {
    assert(FileResolver.parseDatasetOwnerAndName(null).isEmpty)
  }

  "resolveRepositoryVersion" should "resolve a dataset version path to its repository and commit" in {
    assert(
      FileResolver.resolveRepositoryVersion("/dataset/test_user@test.com/test_dataset/v2")
        == ((testDataset.getRepositoryName, testDatasetVersion2.getVersionHash))
    )
  }

  it should "resolve a model version path to its repository and commit" in {
    assert(
      FileResolver.resolveRepositoryVersion("/model/test_user@test.com/test_model/v1")
        == ((testModel.getRepositoryName, testModelVersion1.getVersionHash))
    )
  }

  it should "resolve a legacy unprefixed dataset version path" in {
    assert(
      FileResolver.resolveRepositoryVersion("/test_user@test.com/test_dataset/v1")
        == ((testDataset.getRepositoryName, testDatasetVersion1.getVersionHash))
    )
  }

  it should "reject a path that names a file rather than a version" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveRepositoryVersion(dataset1TxtFilePath)
    }
  }

  it should "reject a path that stops short of a version" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveRepositoryVersion("/dataset/test_user@test.com/test_dataset")
    }
  }

  it should "reject an empty or null path" in {
    assertThrows[FileNotFoundException](FileResolver.resolveRepositoryVersion(""))
    assertThrows[FileNotFoundException](FileResolver.resolveRepositoryVersion(null))
  }

  it should "reject a version that does not exist" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveRepositoryVersion("/dataset/test_user@test.com/test_dataset/v99")
    }
  }

  it should "not resolve a model under the dataset prefix" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolveRepositoryVersion("/dataset/test_user@test.com/test_model/v1")
    }
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

}
