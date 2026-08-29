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

package org.apache.texera.web.resource.dashboard.admin.user

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  DatasetDao,
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  User,
  Workflow,
  WorkflowExecutions,
  WorkflowOfUser,
  WorkflowUserAccess,
  WorkflowVersion
}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.UUID
import javax.ws.rs.{BadRequestException, WebApplicationException}
import scala.jdk.CollectionConverters._

class AdminUserResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val primaryUid = 90000 + scala.util.Random.nextInt(5000)
  private val secondaryUid = primaryUid + 1
  private val testWid = 90000 + scala.util.Random.nextInt(5000)

  private var userDao: UserDao = _
  private var datasetDao: DatasetDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var workflowExecutionsDao: WorkflowExecutionsDao = _
  private var workflowOfUserDao: WorkflowOfUserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _

  private val resource = new AdminUserResource

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
    datasetDao = new DatasetDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  // Wipe everything this spec seeds, children before parents, after each test.
  private def cleanup(): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.UID.in(primaryUid, secondaryUid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW_VERSION).where(WORKFLOW_VERSION.WID.eq(testWid)).execute()
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(testWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW_OF_USER).where(WORKFLOW_OF_USER.WID.eq(testWid)).execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWid)).execute()
    getDSLContext
      .deleteFrom(DATASET)
      .where(DATASET.OWNER_UID.in(primaryUid, secondaryUid))
      .execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.in(primaryUid, secondaryUid)).execute()
    getDSLContext
      .deleteFrom(USER)
      .where(USER.ROLE.eq(UserRoleEnum.INACTIVE).and(USER.NAME.like("User%")))
      .execute()
  }

  private def makeUser(uid: Int, name: String, role: UserRoleEnum = UserRoleEnum.REGULAR): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(
      s"admin_user_spec_${uid}_${UUID.randomUUID().toString.substring(0, 8)}@example.com"
    )
    user.setRole(role)
    user
  }

  /**
    * Seed a credential row. `password` is left null for external providers because
    * ck_provider_credential requires a password for LOCAL and only for LOCAL. The
    * auth_provider FK is ON DELETE CASCADE, so `cleanup`'s user delete clears these.
    */
  private def seedProvider(
      uid: Int,
      providerType: ProviderTypeEnum,
      providerId: String,
      password: String = null
  ): Unit =
    getDSLContext
      .insertInto(AUTH_PROVIDER)
      .set(AUTH_PROVIDER.UID, Integer.valueOf(uid))
      .set(AUTH_PROVIDER.PROVIDER_TYPE, providerType)
      .set(AUTH_PROVIDER.PROVIDER_ID, providerId)
      .set(AUTH_PROVIDER.PASSWORD, password)
      .execute()

  private def seedWorkflow(): Workflow = {
    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName("admin_user_spec_wf_" + UUID.randomUUID().toString.substring(0, 8))
    workflow.setContent("{}")
    workflow.setDescription("desc")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(workflow)
    workflow
  }

  private def seedDataset(uid: Int): Dataset = {
    val dataset = new Dataset
    dataset.setOwnerUid(uid)
    dataset.setName("admin_user_spec_ds_" + UUID.randomUUID().toString.substring(0, 8))
    dataset.setRepositoryName("repo-" + UUID.randomUUID().toString.substring(0, 8))
    dataset.setIsPublic(false)
    dataset.setIsDownloadable(true)
    dataset.setDescription("")
    dataset.setCreationTime(new Timestamp(System.currentTimeMillis()))
    datasetDao.insert(dataset)
    dataset
  }

  private def seedExecution(uid: Int): WorkflowExecutions = {
    seedWorkflow()
    val version = new WorkflowVersion
    version.setWid(testWid)
    version.setContent("{}")
    version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflowVersionDao.insert(version)

    val execution = new WorkflowExecutions
    execution.setVid(version.getVid)
    execution.setUid(uid)
    execution.setStatus(0.toByte)
    execution.setResult("")
    execution.setLogLocation("")
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("admin_user_spec_exec")
    execution.setEnvironmentVersion("test-env-1.0")
    workflowExecutionsDao.insert(execution)
    execution
  }

  override protected def afterEach(): Unit = cleanup()

  // ─── list ───────────────────────────────────────────────────────────────

  "list" should "return the seeded users with their name, email, and role" in {
    val a = makeUser(primaryUid, "alice", UserRoleEnum.ADMIN)
    val b = makeUser(secondaryUid, "bob", UserRoleEnum.REGULAR)
    userDao.insert(a)
    userDao.insert(b)

    val listed = resource.list().asScala
    val alice = listed.find(_.uid == primaryUid)
    val bob = listed.find(_.uid == secondaryUid)

    alice.map(u => (u.name, u.email, u.role)) shouldBe Some(
      ("alice", a.getEmail, UserRoleEnum.ADMIN)
    )
    bob.map(u => (u.name, u.email, u.role)) shouldBe Some(("bob", b.getEmail, UserRoleEnum.REGULAR))
  }

  it should "not return a user that has not been seeded" in {
    resource.list().asScala.exists(_.uid == primaryUid) shouldBe false
  }

  // The projection maps onto UserInfo positionally, so a column landing on the wrong field is
  // silent. Nothing else observes it — pin it here for a user holding both credential kinds,
  // which also proves the LOCAL row does not leak into the GOOGLE-joined column.
  it should "report the google id and the avatar for a user with LOCAL and GOOGLE rows" in {
    val user = makeUser(primaryUid, "dual")
    user.setAvatar("avatar-blob")
    userDao.insert(user)
    seedProvider(primaryUid, ProviderTypeEnum.LOCAL, "dual-handle", password = "hashed")
    seedProvider(primaryUid, ProviderTypeEnum.GOOGLE, "google-sub-dual")

    val listed = resource.list().asScala.find(_.uid == primaryUid)

    listed.map(u => (u.name, u.googleId, u.avatar)) shouldBe Some(
      ("dual", "google-sub-dual", "avatar-blob")
    )
  }

  it should "leave the google id null for a user with no auth_provider rows" in {
    userDao.insert(makeUser(primaryUid, "credential-less"))

    resource.list().asScala.find(_.uid == primaryUid).map(_.googleId) shouldBe Some(null)
  }

  // ─── addUser ────────────────────────────────────────────────────────────

  "addUser" should "persist a new INACTIVE user with a generated name" in {
    val before = userDao.fetchByRole(UserRoleEnum.INACTIVE).size()

    resource.addUser()

    val after = userDao.fetchByRole(UserRoleEnum.INACTIVE)
    after.size() shouldBe before + 1
    after.asScala.exists(_.getName.startsWith("User")) shouldBe true
  }

  // The point of the change: this endpoint was the only one that produced an account holding a
  // credential with no email address on file. The credential was unusable anyway — a generated
  // password the method discarded, which no endpoint resets or reveals — so dropping it costs
  // nothing and is what keeps "every account holding a credential has an address" true.
  it should "not give the new row a credential" in {
    resource.addUser()

    val added = userDao
      .fetchByRole(UserRoleEnum.INACTIVE)
      .asScala
      .filter(_.getName.startsWith("User"))

    added should not be empty
    added.foreach { u =>
      u.getEmail shouldBe null
      getDSLContext.fetchExists(
        getDSLContext.selectFrom(AUTH_PROVIDER).where(AUTH_PROVIDER.UID.eq(u.getUid))
      ) shouldBe false
    }
  }

  // Left as an ordinary row on purpose: marking it a placeholder would stop an admin pre-sharing
  // with the address before its owner first signs in (DatasetAccessResource refuses placeholders).
  it should "not mark the new row a placeholder" in {
    resource.addUser()

    userDao
      .fetchByRole(UserRoleEnum.INACTIVE)
      .asScala
      .filter(_.getName.startsWith("User"))
      .foreach(_.getIsPlaceholder shouldBe false)
  }

  // ─── updateUser ─────────────────────────────────────────────────────────

  "updateUser" should "update a user's editable fields and round-trip via a re-read" in {
    val user = makeUser(primaryUid, "original", UserRoleEnum.REGULAR)
    userDao.insert(user)

    val edit = new User
    edit.setUid(primaryUid)
    edit.setName("renamed")
    edit.setEmail(user.getEmail) // unchanged email → no conflict
    edit.setRole(UserRoleEnum.REGULAR) // unchanged role → no e-mail side effect
    edit.setComment("a new comment")
    resource.updateUser(edit)

    val reread = userDao.fetchOneByUid(primaryUid)
    reread.getName shouldBe "renamed"
    reread.getComment shouldBe "a new comment"
    reread.getRole shouldBe UserRoleEnum.REGULAR
  }

  it should "reject an update whose email already belongs to another user" in {
    val alice = makeUser(primaryUid, "alice", UserRoleEnum.REGULAR)
    val bob = makeUser(secondaryUid, "bob", UserRoleEnum.REGULAR)
    userDao.insert(alice)
    userDao.insert(bob)

    val edit = new User
    edit.setUid(secondaryUid) // updating bob…
    edit.setName("bob")
    edit.setEmail(alice.getEmail) // …to alice's email → conflict
    edit.setRole(UserRoleEnum.REGULAR)

    a[WebApplicationException] should be thrownBy resource.updateUser(edit)
  }

  // ─── updateUser: the email safeguard ──────────────────────────────────────

  // An unclaimed stub has no address, and the admin table renders that as a blank cell beside a
  // role dropdown. Past INACTIVE the address stops being cosmetic — dataset paths are built from
  // it, and the role-change notification is sent to it.
  it should "refuse to activate an account that has no email address" in {
    val emailless = makeUser(primaryUid, "no_address", UserRoleEnum.INACTIVE)
    emailless.setEmail(null)
    userDao.insert(emailless)

    val edit = new User
    edit.setUid(primaryUid)
    edit.setName("no_address")
    edit.setEmail(null)
    edit.setRole(UserRoleEnum.REGULAR)

    a[WebApplicationException] should be thrownBy resource.updateUser(edit)
    userDao.fetchOneByUid(primaryUid).getRole shouldBe UserRoleEnum.INACTIVE
  }

  // The other half of the guarantee: closing `addUser` stops emailless credentialed accounts being
  // created, and this stops one being made by taking an address back off an account that has one.
  it should "refuse to remove an address an account already has" in {
    val user = makeUser(primaryUid, "has_address", UserRoleEnum.REGULAR)
    userDao.insert(user)

    val edit = new User
    edit.setUid(primaryUid)
    edit.setName("has_address")
    edit.setEmail("   ")
    edit.setRole(UserRoleEnum.INACTIVE)

    a[WebApplicationException] should be thrownBy resource.updateUser(edit)
    userDao.fetchOneByUid(primaryUid).getEmail shouldBe user.getEmail
  }

  it should "allow editing an emailless account that stays inactive" in {
    val emailless = makeUser(primaryUid, "no_address", UserRoleEnum.INACTIVE)
    emailless.setEmail(null)
    userDao.insert(emailless)

    val edit = new User
    edit.setUid(primaryUid)
    edit.setName("renamed")
    edit.setEmail(null)
    edit.setRole(UserRoleEnum.INACTIVE)
    edit.setComment("waiting on an address")
    resource.updateUser(edit)

    userDao.fetchOneByUid(primaryUid).getName shouldBe "renamed"
  }

  // ─── getCreatedDatasets ───────────────────────────────────────────────────

  "getCreatedDatasets" should "return an empty list for a user with no datasets" in {
    userDao.insert(makeUser(primaryUid, "dataset_user"))
    resource.getCreatedDatasets(primaryUid) shouldBe empty
  }

  it should "reject a missing user_id with a BadRequestException" in {
    assertThrows[BadRequestException](resource.getCreatedDatasets(null))
  }

  it should "return only the datasets owned by the queried user" in {
    userDao.insert(makeUser(primaryUid, "dataset_owner"))
    userDao.insert(makeUser(secondaryUid, "other_owner"))
    val owned = seedDataset(primaryUid)
    seedDataset(secondaryUid)

    val created = resource.getCreatedDatasets(primaryUid)
    created.map(_.name) shouldBe List(owned.getName)
    created.head.size shouldBe 0L
  }

  // ─── getCreatedWorkflow ───────────────────────────────────────────────────

  "getCreatedWorkflow" should "return an empty list for a user with no created workflows" in {
    userDao.insert(makeUser(primaryUid, "creator"))
    resource.getCreatedWorkflow(primaryUid) shouldBe empty
  }

  it should "return the workflows the user created" in {
    userDao.insert(makeUser(primaryUid, "creator"))
    val workflow = seedWorkflow()
    val ownership = new WorkflowOfUser
    ownership.setUid(primaryUid)
    ownership.setWid(testWid)
    workflowOfUserDao.insert(ownership)

    val created = resource.getCreatedWorkflow(primaryUid)
    created.map(_.workflowId) shouldBe List(Integer.valueOf(testWid))
    created.head.workflowName shouldBe workflow.getName
  }

  // ─── getAccessedWorkflow ──────────────────────────────────────────────────

  "getAccessedWorkflow" should "return an empty list for a user with no workflow access" in {
    userDao.insert(makeUser(primaryUid, "viewer"))
    resource.getAccessedWorkflow(primaryUid).asScala shouldBe empty
  }

  it should "return the workflow ids the user can access" in {
    userDao.insert(makeUser(primaryUid, "viewer"))
    seedWorkflow()
    val access = new WorkflowUserAccess
    access.setUid(primaryUid)
    access.setWid(testWid)
    access.setPrivilege(PrivilegeEnum.READ)
    workflowUserAccessDao.insert(access)

    resource.getAccessedWorkflow(primaryUid).asScala should contain(Integer.valueOf(testWid))
  }

  // ─── getUserQuota ─────────────────────────────────────────────────────────

  "getUserQuota" should "return an empty array for a user with no executions" in {
    userDao.insert(makeUser(primaryUid, "quota_user"))
    resource.getUserQuota(primaryUid) shouldBe empty
  }

  it should "return one quota entry per execution the user owns" in {
    userDao.insert(makeUser(primaryUid, "quota_user"))
    val execution = seedExecution(primaryUid)

    val quota = resource.getUserQuota(primaryUid)
    quota should have length 1
    quota.head.eid shouldBe execution.getEid
    quota.head.workflowId shouldBe Integer.valueOf(testWid)
    // No operator_port/operator rows seeded → result and log sizes are zero.
    quota.head.resultBytes shouldBe 0L
    quota.head.logBytes shouldBe 0L
  }

  // ─── deleteCollection ─────────────────────────────────────────────────────

  "deleteCollection" should "delete the target execution row" in {
    userDao.insert(makeUser(primaryUid, "collection_user"))
    val execution = seedExecution(primaryUid)
    workflowExecutionsDao.fetchOneByEid(execution.getEid) should not be null

    resource.deleteCollection(execution.getEid)

    workflowExecutionsDao.fetchOneByEid(execution.getEid) shouldBe null
  }
}
