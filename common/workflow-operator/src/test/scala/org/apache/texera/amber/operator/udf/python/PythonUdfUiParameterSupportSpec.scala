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

package org.apache.texera.amber.operator.udf.python

import org.apache.commons.vfs2.FileNotFoundException
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Forces a Python UDF's execution-time binding against real dataset and model rows. */
class PythonUdfUiParameterSupportSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val owner: User = {
    val user = new User
    user.setUid(1)
    user.setName("owner")
    user.setEmail("owner@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  private val dataset: Dataset = {
    val dataset = new Dataset
    dataset.setDid(1)
    dataset.setName("sales")
    dataset.setRepositoryName("dataset-1")
    dataset.setDescription("")
    dataset.setIsPublic(false)
    dataset.setOwnerUid(owner.getUid)
    dataset
  }

  private val datasetVersion: DatasetVersion = {
    val version = new DatasetVersion
    version.setDvid(1)
    version.setDid(dataset.getDid)
    version.setName("v1")
    version.setCreatorUid(owner.getUid)
    version.setVersionHash("d4t4")
    version
  }

  private val model: Model = {
    val model = new Model
    model.setMid(1)
    model.setName("iris")
    model.setRepositoryName("model-1")
    model.setDescription("")
    model.setIsPublic(false)
    model.setIsDownloadable(true)
    model.setOwnerUid(owner.getUid)
    model
  }

  private val modelVersion: ModelVersion = {
    val version = new ModelVersion
    version.setMvid(1)
    version.setMid(model.getMid)
    version.setName("v2")
    version.setCreatorUid(owner.getUid)
    version.setVersionHash("m0d3l")
    version
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val config = getDSLContext.configuration()
    new UserDao(config).insert(owner)
    new DatasetDao(config).insert(dataset)
    new DatasetVersionDao(config).insert(datasetVersion)
    new ModelDao(config).insert(model)
    new ModelVersionDao(config).insert(modelVersion)
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  private val code =
    """from pytexera import *
      |
      |class ProcessTupleOperator(UDFOperatorV2):
      |    def process_tuple(self, tuple_, port):
      |        yield tuple_
      |""".stripMargin

  private def parameter(
      name: String,
      attributeType: AttributeType,
      inputType: String,
      value: String
  ): UiUDFParameter = {
    val parameter = new UiUDFParameter
    parameter.attribute = new Attribute(name, attributeType)
    parameter.inputType = inputType
    parameter.value = value
    parameter
  }

  private def udfWith(parameters: UiUDFParameter*): PythonUDFOpDescV2 = {
    val udf = new PythonUDFOpDescV2
    udf.code = code
    udf.uiParameters = parameters.toList
    udf
  }

  "A Python UDF's execution-time binding" should
    "hand workers the mount directory of every resource it names" in {
    val count = parameter("count", AttributeType.INTEGER, "", "7")
    val op = udfWith(
      parameter("DATA", AttributeType.STRING, "dataset", "/dataset/owner@test.com/sales/v1"),
      parameter("MODEL", AttributeType.STRING, "model", "/model/owner@test.com/iris/v2"),
      count
    ).getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))

    op.mountLocators shouldBe Set("dataset-1:d4t4", "model-1:m0d3l")
    val bound = PythonUdfUiParameterInjector.inject(
      code,
      List(
        parameter("DATA", AttributeType.STRING, "dataset", "/mnt/texera-mounts/dataset-1/d4t4"),
        parameter("MODEL", AttributeType.STRING, "model", "/mnt/texera-mounts/model-1/m0d3l"),
        count
      )
    )
    op.executableOpExecInitInfo shouldBe OpExecWithCode(bound, "python")
    // The compile-time view still names the versions; only the run sees the mounts.
    op.opExecInitInfo should not be op.executableOpExecInitInfo
  }

  it should "report a version that no longer exists when the run starts, not while editing" in {
    val op = udfWith(
      parameter("DATA", AttributeType.STRING, "dataset", "/dataset/owner@test.com/sales/v9")
    ).getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))

    val failure = the[FileNotFoundException] thrownBy op.mountLocators
    failure.getMessage should include("/dataset/owner@test.com/sales/v9")
  }
}
