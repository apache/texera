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
  DatasetUserAccessDao,
  DatasetVersionDao,
  UserDao,
  WorkflowComputingUnitDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  ComputingUnitUserAccess,
  Dataset,
  DatasetUserAccess,
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
import scala.reflect.ClassTag

class ComputingUnitMountResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  /** A regular user whose email is `name@test.com`. */
  private def makeUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  /** A private dataset stored as repository `repo` and owned by `ownerUid`. */
  private def makeDataset(did: Int, name: String, repo: String, ownerUid: Int): Dataset = {
    val dataset = new Dataset
    dataset.setDid(did)
    dataset.setName(name)
    dataset.setDescription("")
    dataset.setRepositoryName(repo)
    dataset.setOwnerUid(ownerUid)
    dataset.setIsPublic(false)
    dataset
  }

  /** Version `v1` of dataset `did`, created by `creatorUid`, at commit `hash`. */
  private def makeVersion(dvid: Int, did: Int, creatorUid: Int, hash: String): DatasetVersion = {
    val version = new DatasetVersion
    version.setDvid(dvid)
    version.setDid(did)
    version.setName("v1")
    version.setCreatorUid(creatorUid)
    version.setVersionHash(hash)
    version
  }

  private val owner: User = makeUser(1, "owner")
  private val reader: User = makeUser(2, "reader")
  private val stranger: User = makeUser(3, "stranger")

  private val ownedDataset: Dataset = makeDataset(1, "owned", "dataset-1", owner.getUid)
  private val ownedVersion: DatasetVersion =
    makeVersion(1, ownedDataset.getDid, owner.getUid, "abc123")

  // Another user's private dataset: the only fixture whose refusal comes from read access
  // rather than from the repository not existing.
  private val strangersDataset: Dataset = makeDataset(2, "private", "dataset-2", stranger.getUid)
  private val strangersVersion: DatasetVersion =
    makeVersion(2, strangersDataset.getDid, stranger.getUid, "def456")

  private val computingUnit: WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit
    unit.setCuid(7)
    unit.setUid(owner.getUid)
    unit.setName("test-cu")
    unit.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    unit
  }

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

  /** Expects `mount` to throw `T` before the node mounter is asked for anything. */
  private def expectMountRejected[T <: Throwable: ClassTag](
      user: User = owner,
      cuid: Int = computingUnit.getCuid,
      repo: String = ownedDataset.getRepositoryName,
      commit: String = ownedVersion.getVersionHash,
      build: MounterClient => ComputingUnitMountResource = resource(_)
  ): Unit = {
    val mounter = new RecordingMounter
    a[T] should be thrownBy build(mounter).mount(cuid, MountRequest(repo, commit), sessionOf(user))
    mounter.mounts shouldBe empty
  }

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
    // The reader may read the owned dataset, so only the unit's WRITE requirement can refuse
    // the reader's mount.
    val datasetAccess = new DatasetUserAccess()
    datasetAccess.setDid(ownedDataset.getDid)
    datasetAccess.setUid(reader.getUid)
    datasetAccess.setPrivilege(PrivilegeEnum.READ)
    new DatasetUserAccessDao(getDSLContext.configuration()).insert(datasetAccess)
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
    expectMountRejected[ForbiddenException](user = reader)
  }

  it should "refuse a repository the user cannot read" in {
    // The mounter authorizes nothing, so a refusal has to happen before it is asked.
    expectMountRejected[ForbiddenException](repo = "dataset-2", commit = "def456")
  }

  it should "refuse a repository that does not exist" in {
    expectMountRejected[ForbiddenException](repo = "dataset-404")
  }

  it should "refuse a commit that belongs to another repository" in {
    expectMountRejected[ForbiddenException](commit = "def456")
  }

  it should "refuse a user with no access to the computing unit" in {
    expectMountRejected[ForbiddenException](user = stranger)
  }

  it should "refuse a computing unit that does not exist" in {
    expectMountRejected[ForbiddenException](cuid = 999)
  }

  it should "report a misconfigured port rather than dialling a wrong one" in {
    expectMountRejected[InternalServerErrorException](build = resource(_, mounterPort = None))
  }

  it should "report a missing file-service address rather than mounting against nothing" in {
    expectMountRejected[InternalServerErrorException](build = resource(_, fileServiceUrl = None))
  }

  it should "answer plainly when the deployment did not enable mounting" in {
    expectMountRejected[ServiceUnavailableException](build = resource(_, mounterEnabled = false))
  }

  it should "refuse while the unit's pod is not on a node yet" in {
    expectMountRejected[BadRequestException](build = resource(_, nodeLocator = notScheduled))
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
