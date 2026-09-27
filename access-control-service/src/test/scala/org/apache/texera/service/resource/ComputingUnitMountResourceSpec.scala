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

import jakarta.ws.rs.{
  BadRequestException,
  ForbiddenException,
  InternalServerErrorException,
  ServiceUnavailableException
}
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{
  PrivilegeEnum,
  UserRoleEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  ComputingUnitUserAccessDao,
  DatasetDao,
  DatasetVersionDao,
  UserDao,
  WorkflowComputingUnitDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  ComputingUnitUserAccess,
  Dataset,
  DatasetVersion,
  User,
  WorkflowComputingUnit
}
import org.apache.texera.service.resource.ComputingUnitMountResource.MountRequest
import org.apache.texera.service.util.{ComputingUnitNodeLocator, MounterClient}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class ComputingUnitMountResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val owner: User = {
    val user = new User
    user.setUid(1)
    user.setName("owner")
    user.setEmail("owner@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val reader: User = {
    val user = new User
    user.setUid(2)
    user.setName("reader")
    user.setEmail("reader@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val stranger: User = {
    val user = new User
    user.setUid(3)
    user.setName("stranger")
    user.setEmail("stranger@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val ownedDataset: Dataset = {
    val dataset = new Dataset
    dataset.setDid(1)
    dataset.setName("owned")
    dataset.setDescription("")
    dataset.setRepositoryName("dataset-1")
    dataset.setOwnerUid(owner.getUid)
    dataset.setIsPublic(false)
    dataset
  }

  private val ownedVersion: DatasetVersion = {
    val version = new DatasetVersion
    version.setDvid(1)
    version.setDid(ownedDataset.getDid)
    version.setName("v1")
    version.setCreatorUid(owner.getUid)
    version.setVersionHash("abc123")
    version
  }

  // Another user's private dataset: the only fixture whose refusal comes from read access
  // rather than from the repository not existing.
  private val strangersDataset: Dataset = {
    val dataset = new Dataset
    dataset.setDid(2)
    dataset.setName("private")
    dataset.setDescription("")
    dataset.setRepositoryName("dataset-2")
    dataset.setOwnerUid(stranger.getUid)
    dataset.setIsPublic(false)
    dataset
  }

  private val strangersVersion: DatasetVersion = {
    val version = new DatasetVersion
    version.setDvid(2)
    version.setDid(strangersDataset.getDid)
    version.setName("v1")
    version.setCreatorUid(stranger.getUid)
    version.setVersionHash("def456")
    version
  }

  private val computingUnit: WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit
    unit.setCuid(7)
    unit.setUid(owner.getUid)
    unit.setName("test-cu")
    unit.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    unit
  }

  /** Records what the resource asked the node mounter to do, without any HTTP. */
  /** Records what the resource asked the node mounter to do, without any HTTP. */
  private class RecordingMounter extends MounterClient("/nonexistent-token") {
    val mounts: mutable.Buffer[(String, Int, String, String, String, String, String)] =
      mutable.Buffer()
    var failWith: Option[Throwable] = None

    override def mount(
        nodeIp: String,
        port: Int,
        cuid: String,
        repositoryName: String,
        commitHash: String,
        jwt: String,
        fileServiceBase: String
    ): String = {
      failWith.foreach(throw _)
      mounts += ((nodeIp, port, cuid, repositoryName, commitHash, jwt, fileServiceBase))
      s"/var/lib/texera-mounts/$cuid/$repositoryName/$commitHash"
    }

  }

  private val scheduledOnNode = new ComputingUnitNodeLocator(_ => None) {
    override def nodeIpOf(cuid: Int): Option[String] = Some("10.0.0.4")
  }

  private val notScheduled = new ComputingUnitNodeLocator(_ => None) {
    override def nodeIpOf(cuid: Int): Option[String] = None
  }

  private def resource(
      mounter: MounterClient,
      mounterEnabled: Boolean = true,
      nodeLocator: ComputingUnitNodeLocator = scheduledOnNode,
      mounterPort: Option[Int] = Some(8100),
      fileServiceUrl: Option[String] = Some("http://file-service-svc:9092")
  ) =
    new ComputingUnitMountResource(
      mounterEnabled,
      mounterPort,
      fileServiceUrl,
      nodeLocator,
      mounter
    )

  private def sessionOf(user: User) = new SessionUser(user)

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(owner)
    userDao.insert(reader)
    userDao.insert(stranger)
    new WorkflowComputingUnitDao(getDSLContext.configuration()).insert(computingUnit)
    val datasetDao = new DatasetDao(getDSLContext.configuration())
    datasetDao.insert(ownedDataset)
    datasetDao.insert(strangersDataset)
    val versionDao = new DatasetVersionDao(getDSLContext.configuration())
    versionDao.insert(ownedVersion)
    versionDao.insert(strangersVersion)
    val access = new ComputingUnitUserAccess()
    access.setCuid(computingUnit.getCuid)
    access.setUid(reader.getUid)
    access.setPrivilege(PrivilegeEnum.READ)
    new ComputingUnitUserAccessDao(getDSLContext.configuration()).insert(access)
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  "mount" should "forward the request to the mounter on the unit's own node" in {
    val mounter = new RecordingMounter
    val info = resource(mounter).mount(7, MountRequest("dataset-1", "abc123"), sessionOf(owner))

    info.repositoryName shouldBe "dataset-1"
    info.commitHash shouldBe "abc123"
    info.mountPath shouldBe "/var/lib/texera-mounts/7/dataset-1/abc123"

    val (nodeIp, port, cuid, repository, commit, jwt, fileServiceBase) = mounter.mounts.head
    nodeIp shouldBe "10.0.0.4"
    port shouldBe 8100
    cuid shouldBe "7"
    repository shouldBe "dataset-1"
    commit shouldBe "abc123"
    fileServiceBase shouldBe "http://file-service-svc:9092"
    jwt should not be empty
  }

  // The only fixture with READ and not WRITE: tells "any access is enough" apart from
  // "write access is required".
  it should "refuse a read-only sharee, who may use the unit but not change what it sees" in {
    val mounter = new RecordingMounter
    a[ForbiddenException] should be thrownBy
      resource(mounter).mount(7, MountRequest("dataset-1", "abc123"), sessionOf(reader))
    mounter.mounts shouldBe empty
  }

  it should "refuse a repository the user cannot read" in {
    val mounter = new RecordingMounter
    a[ForbiddenException] should be thrownBy
      resource(mounter).mount(7, MountRequest("dataset-2", "def456"), sessionOf(owner))
    // The mounter authorizes nothing, so a refusal has to happen before it is asked.
    mounter.mounts shouldBe empty
  }

  it should "refuse a repository that does not exist" in {
    val mounter = new RecordingMounter
    a[ForbiddenException] should be thrownBy
      resource(mounter).mount(7, MountRequest("dataset-404", "abc123"), sessionOf(owner))
    mounter.mounts shouldBe empty
  }

  it should "refuse a commit that belongs to another repository" in {
    val mounter = new RecordingMounter
    a[ForbiddenException] should be thrownBy
      resource(mounter).mount(7, MountRequest("dataset-1", "def456"), sessionOf(owner))
    mounter.mounts shouldBe empty
  }

  it should "refuse a user with no access to the computing unit" in {
    val mounter = new RecordingMounter
    a[ForbiddenException] should be thrownBy
      resource(mounter).mount(7, MountRequest("dataset-1", "abc123"), sessionOf(stranger))
    mounter.mounts shouldBe empty
  }

  it should "refuse a computing unit that does not exist" in {
    val mounter = new RecordingMounter
    a[ForbiddenException] should be thrownBy
      resource(mounter).mount(999, MountRequest("dataset-1", "abc123"), sessionOf(owner))
    mounter.mounts shouldBe empty
  }

  it should "report a misconfigured port rather than dialling a wrong one" in {
    val mounter = new RecordingMounter
    an[InternalServerErrorException] should be thrownBy
      resource(mounter, mounterPort = None)
        .mount(7, MountRequest("dataset-1", "abc123"), sessionOf(owner))
    mounter.mounts shouldBe empty
  }

  it should "report a missing file-service address rather than mounting against nothing" in {
    val mounter = new RecordingMounter
    an[InternalServerErrorException] should be thrownBy
      resource(mounter, fileServiceUrl = None)
        .mount(7, MountRequest("dataset-1", "abc123"), sessionOf(owner))
    mounter.mounts shouldBe empty
  }

  it should "answer plainly when the deployment did not enable mounting" in {
    val mounter = new RecordingMounter
    a[ServiceUnavailableException] should be thrownBy
      resource(mounter, mounterEnabled = false)
        .mount(7, MountRequest("dataset-1", "abc123"), sessionOf(owner))
    mounter.mounts shouldBe empty
  }

  it should "refuse while the unit's pod is not on a node yet" in {
    val mounter = new RecordingMounter
    a[BadRequestException] should be thrownBy
      resource(mounter, nodeLocator = notScheduled)
        .mount(7, MountRequest("dataset-1", "abc123"), sessionOf(owner))
    mounter.mounts shouldBe empty
  }

  it should "report a rejected path as a bad request rather than a server error" in {
    val mounter = new RecordingMounter
    mounter.failWith = Some(new IllegalArgumentException("repositoryName must be a single segment"))
    val failure = the[BadRequestException] thrownBy
      resource(mounter).mount(7, MountRequest("../evil", "abc123"), sessionOf(owner))
    failure.getMessage should include("repositoryName")
  }

  it should "relay a refusal from the mounter as a bad request" in {
    val mounter = new RecordingMounter
    mounter.failWith = Some(new MounterClient.MounterRequestException(400, "mounter said no"))
    a[BadRequestException] should be thrownBy
      resource(mounter).mount(7, MountRequest("dataset-1", "abc123"), sessionOf(owner))
  }
}
