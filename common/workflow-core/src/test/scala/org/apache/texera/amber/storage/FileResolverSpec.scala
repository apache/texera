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
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  DatasetDao,
  DatasetVersionDao,
  ModelDao,
  ModelVersionDao,
  UserDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  DatasetVersion,
  Model,
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

  private val localCsvFilePath = "common/workflow-core/src/test/resources/country_sales_small.csv"

  private val datasetACsvFilePath = "/datasets/test_user@test.com/test_dataset/v2/directory/a.csv"

  private val dataset1TxtFilePath = "/datasets/test_user@test.com/test_dataset/v1/1.txt"

  private val modelWeightsFilePath = "/models/test_user@test.com/test_model/v2/weights/model.pt"

  private val modelReadmeFilePath = "/models/test_user@test.com/test_model/v1/README.md"

  // Unprefixed form (no resource-type segment); no longer resolvable as a dataset.
  private val unprefixedDataset1TxtFilePath = "/test_user@test.com/test_dataset/v1/1.txt"

  // The leading segment is not a known resource type, so this is not a resolvable path.
  private val unknownPrefixFilePath = "/notAResourceType/test_user@test.com/test_dataset/v1/1.txt"

  // A model name presented under the datasets prefix must not resolve as a dataset.
  private val modelNameUnderDatasetPrefix = "/datasets/test_user@test.com/test_model/v1/README.md"

  // A dataset name presented under the models prefix must not resolve as a model.
  private val datasetNameUnderModelPrefix = "/models/test_user@test.com/test_dataset/v1/1.txt"

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
  }

  "FileResolver" should "resolve local file correctly" in {
    val localUri = FileResolver.resolve(localCsvFilePath)

    assert(localUri == Paths.get(localCsvFilePath).toUri)
  }

  "FileResolver" should "resolve dataset file correctly" in {
    val datasetACsvUri = FileResolver.resolve(datasetACsvFilePath)
    val dataset1TxtUri = FileResolver.resolve(dataset1TxtFilePath)

    assert(
      datasetACsvUri.toString == f"${FileResolver.DATASET_FILE_URI_SCHEME}:///${testDataset.getRepositoryName}/${testDatasetVersion2.getVersionHash}/directory/a.csv"
    )
    assert(
      dataset1TxtUri.toString == f"${FileResolver.DATASET_FILE_URI_SCHEME}:///${testDataset.getRepositoryName}/${testDatasetVersion1.getVersionHash}/1.txt"
    )
  }

  "FileResolver" should "resolve model file correctly" in {
    val modelWeightsUri = FileResolver.resolve(modelWeightsFilePath)
    val modelReadmeUri = FileResolver.resolve(modelReadmeFilePath)

    assert(
      modelWeightsUri.toString == f"${FileResolver.MODEL_FILE_URI_SCHEME}:///${testModel.getRepositoryName}/${testModelVersion2.getVersionHash}/weights/model.pt"
    )
    assert(
      modelReadmeUri.toString == f"${FileResolver.MODEL_FILE_URI_SCHEME}:///${testModel.getRepositoryName}/${testModelVersion1.getVersionHash}/README.md"
    )
  }

  "FileResolver" should "not resolve a path without a resource-type prefix" in {
    // Without a leading resource-type segment the path is not resolvable
    assertThrows[FileNotFoundException] {
      FileResolver.resolve(unprefixedDataset1TxtFilePath)
    }
  }

  "FileResolver" should "not resolve a path whose prefix is not a known resource type" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolve(unknownPrefixFilePath)
    }
  }

  "FileResolver" should "keep datasets and models isolated by prefix" in {
    // a real dataset name under the models prefix is not a model, and vice-versa
    assertThrows[FileNotFoundException] {
      FileResolver.resolve(datasetNameUnderModelPrefix)
    }
    assertThrows[FileNotFoundException] {
      FileResolver.resolve(modelNameUnderDatasetPrefix)
    }
  }

  "FileResolver" should "throw not found exception when a prefixed path has too few segments" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolve("/datasets/test_user@test.com/test_dataset")
    }
  }

  "FileResolver" should "throw not found exception" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolve("some/random/path")
    }
  }

  "isFileResolved" should "return true when the path has a non-empty scheme" in {
    assert(FileResolver.isFileResolved("s3://bucket/key"))
    assert(
      FileResolver.isFileResolved(s"${FileResolver.DATASET_FILE_URI_SCHEME}:///repo/hash/file.csv")
    )
  }

  it should "return false when the path has no scheme" in {
    assert(!FileResolver.isFileResolved("some/random/path"))
    assert(!FileResolver.isFileResolved("/test_user@test.com/test_dataset/v1/1.txt"))
  }

  it should "return false for a malformed URI" in {
    // a space is illegal in a URI and makes the java.net.URI constructor throw
    assert(!FileResolver.isFileResolved("has a space"))
  }

  "parseDatasetOwnerAndName" should "extract owner email and dataset name from a valid path" in {
    assert(
      FileResolver.parseDatasetOwnerAndName("/datasets/test_user@test.com/test_dataset/v1/1.txt")
        == Some(("test_user@test.com", "test_dataset"))
    )
    // extra segments beyond the file-relative path are ignored
    assert(
      FileResolver.parseDatasetOwnerAndName("/datasets/owner@x.com/ds/v2/directory/nested/a.csv")
        == Some(("owner@x.com", "ds"))
    )
  }

  it should "return None for an unprefixed path (the datasets prefix is required)" in {
    assert(FileResolver.parseDatasetOwnerAndName(unprefixedDataset1TxtFilePath).isEmpty)
  }

  it should "return None when the prefixed path has too few segments" in {
    assert(FileResolver.parseDatasetOwnerAndName("/datasets/owner@x.com/ds").isEmpty)
    assert(FileResolver.parseDatasetOwnerAndName("owner/dataset").isEmpty)
  }

  it should "return None for a null path" in {
    assert(FileResolver.parseDatasetOwnerAndName(null).isEmpty)
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

}
