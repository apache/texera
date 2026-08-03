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

package org.apache.texera.web.resource.dashboard.user.project

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowOfProjectDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowOfProject,
  WorkflowOfUser,
  WorkflowUserAccess
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.Timestamp
import java.util.UUID
import javax.ws.rs.{BadRequestException, ForbiddenException}
import scala.jdk.CollectionConverters._

class ProjectResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val ownerUid = 12000 + scala.util.Random.nextInt(1000)
  private val strangerUid = 14000 + scala.util.Random.nextInt(1000)
  private val testUids = Seq(ownerUid, strangerUid)

  private var owner: User = _
  private var stranger: User = _
  private var workflowOfProjectDao: WorkflowOfProjectDao = _
  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowOfUserDao: WorkflowOfUserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _
  private var resource: ProjectResource = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    workflowOfProjectDao = new WorkflowOfProjectDao(getDSLContext.configuration())
    userDao = new UserDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())
    resource = new ProjectResource()

    owner = makeUser(ownerUid, "proj_owner")
    stranger = makeUser(strangerUid, "proj_stranger")

    cleanupTestData()
    userDao.insert(owner)
    userDao.insert(stranger)
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

  private def cleanupTestData(): Unit = {
    val ctx = getDSLContext
    // projects owned by our test users (pids are DB-generated)
    val pids = ctx
      .select(PROJECT.PID)
      .from(PROJECT)
      .where(PROJECT.OWNER_ID.in(testUids.map(Integer.valueOf): _*))
      .fetchInto(classOf[Integer])
      .asScala
      .toList
    // workflows our test users came to own
    val wids = ctx
      .select(WORKFLOW_OF_USER.WID)
      .from(WORKFLOW_OF_USER)
      .where(WORKFLOW_OF_USER.UID.in(testUids.map(Integer.valueOf): _*))
      .fetchInto(classOf[Integer])
      .asScala
      .toList

    if (pids.nonEmpty) {
      ctx.deleteFrom(WORKFLOW_OF_PROJECT).where(WORKFLOW_OF_PROJECT.PID.in(pids: _*)).execute()
      ctx.deleteFrom(PROJECT_USER_ACCESS).where(PROJECT_USER_ACCESS.PID.in(pids: _*)).execute()
    }
    ctx
      .deleteFrom(PROJECT_USER_ACCESS)
      .where(PROJECT_USER_ACCESS.UID.in(testUids.map(Integer.valueOf): _*))
      .execute()
    ctx.deleteFrom(PROJECT).where(PROJECT.OWNER_ID.in(testUids.map(Integer.valueOf): _*)).execute()

    if (wids.nonEmpty) {
      ctx.deleteFrom(WORKFLOW_OF_PROJECT).where(WORKFLOW_OF_PROJECT.WID.in(wids: _*)).execute()
      ctx.deleteFrom(WORKFLOW_USER_ACCESS).where(WORKFLOW_USER_ACCESS.WID.in(wids: _*)).execute()
      ctx.deleteFrom(WORKFLOW_OF_USER).where(WORKFLOW_OF_USER.WID.in(wids: _*)).execute()
      ctx.deleteFrom(WORKFLOW).where(WORKFLOW.WID.in(wids: _*)).execute()
    }
    ctx.deleteFrom(USER).where(USER.UID.in(testUids.map(Integer.valueOf): _*)).execute()
  }

  private def makeUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(Integer.valueOf(uid))
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user
  }

  private def session(user: User): SessionUser = new SessionUser(user)

  /** Seeds a workflow owned by the given user with WRITE access (so hasReadAccess passes). */
  private def seedWorkflow(uid: Int): Integer = {
    val wid = Integer.valueOf(16000 + scala.util.Random.nextInt(100000))
    val workflow = new Workflow
    workflow.setWid(wid)
    workflow.setName("wf_" + UUID.randomUUID().toString.substring(0, 8))
    workflow.setContent("""{"operators":[],"links":[]}""")
    workflow.setDescription("")
    workflow.setIsPublic(false)
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(workflow)

    val ownership = new WorkflowOfUser
    ownership.setUid(Integer.valueOf(uid))
    ownership.setWid(wid)
    workflowOfUserDao.insert(ownership)

    val access = new WorkflowUserAccess
    access.setUid(Integer.valueOf(uid))
    access.setWid(wid)
    access.setPrivilege(PrivilegeEnum.WRITE)
    workflowUserAccessDao.insert(access)
    wid
  }

  private def workflowOfProjectCount(wid: Integer, pid: Integer): Int =
    getDSLContext.fetchCount(
      WORKFLOW_OF_PROJECT,
      WORKFLOW_OF_PROJECT.WID.eq(wid).and(WORKFLOW_OF_PROJECT.PID.eq(pid))
    )

  behavior of "ProjectResource"

  it should "create a project owned by the user and make it retrievable with a WRITE access row" in {
    val created = resource.createProject(session(owner), "my_project")

    created.getName shouldBe "my_project"
    created.getOwnerId shouldBe Integer.valueOf(ownerUid)
    resource.getProject(created.getPid).getName shouldBe "my_project"

    val privilege = getDSLContext
      .select(PROJECT_USER_ACCESS.PRIVILEGE)
      .from(PROJECT_USER_ACCESS)
      .where(
        PROJECT_USER_ACCESS.PID
          .eq(created.getPid)
          .and(PROJECT_USER_ACCESS.UID.eq(Integer.valueOf(ownerUid)))
      )
      .fetchOne(0, classOf[PrivilegeEnum])
    privilege shouldBe PrivilegeEnum.WRITE
  }

  it should "list no projects for a user who owns none and all projects once created" in {
    resource.getProjectList(session(stranger)).asScala shouldBe empty

    resource.createProject(session(owner), "p1")
    resource.createProject(session(owner), "p2")

    resource.getProjectList(session(owner)).asScala.map(_.name).toSet shouldBe Set("p1", "p2")
  }

  it should "rename a project and reject a blank name" in {
    val pid = resource.createProject(session(owner), "before").getPid

    resource.updateProjectName(pid, "after")
    resource.getProject(pid).getName shouldBe "after"

    assertThrows[BadRequestException] {
      resource.updateProjectName(pid, "   ")
    }
    // the rejected rename left the previous value intact
    resource.getProject(pid).getName shouldBe "after"
  }

  it should "update a project description via a re-read" in {
    val pid = resource.createProject(session(owner), "p").getPid

    resource.updateProjectDescription(pid, "a new description")

    resource.getProject(pid).getDescription shouldBe "a new description"
  }

  it should "add a workflow to a project, stay idempotent, and reject a user without access" in {
    val pid = resource.createProject(session(owner), "p").getPid
    val wid = seedWorkflow(ownerUid)

    resource.addWorkflowToProject(pid, wid, session(owner))
    workflowOfProjectCount(wid, pid) shouldBe 1

    // a second add for the same pair must not create a duplicate mapping
    resource.addWorkflowToProject(pid, wid, session(owner))
    workflowOfProjectCount(wid, pid) shouldBe 1

    // the stranger has no access to this workflow
    assertThrows[ForbiddenException] {
      resource.addWorkflowToProject(pid, wid, session(stranger))
    }
  }

  it should "remove a workflow-to-project mapping" in {
    val pid = resource.createProject(session(owner), "p").getPid
    val wid = seedWorkflow(ownerUid)
    resource.addWorkflowToProject(pid, wid, session(owner))
    workflowOfProjectCount(wid, pid) shouldBe 1

    resource.deleteWorkflowFromProject(pid, wid)

    workflowOfProjectCount(wid, pid) shouldBe 0
  }

  it should "delete a project" in {
    val pid = resource.createProject(session(owner), "doomed").getPid
    resource.getProject(pid) should not be null

    resource.deleteProject(pid)

    resource.getProject(pid) shouldBe null
  }

  behavior of "ProjectResource.addExportedFileToProject"

  it should "return an empty status when the workflow belongs to no project" in {
    val wid = seedWorkflow(ownerUid)
    ProjectResource.addExportedFileToProject(Integer.valueOf(ownerUid), wid, "out.csv") shouldBe ""
  }

  it should "name the single project the workflow belongs to" in {
    val wid = seedWorkflow(ownerUid)
    val pid = resource.createProject(session(owner), "only_project").getPid
    workflowOfProjectDao.insert(new WorkflowOfProject(wid, pid))

    ProjectResource.addExportedFileToProject(
      Integer.valueOf(ownerUid),
      wid,
      "out.csv"
    ) shouldBe "and added to project: only_project"
  }

  it should "list every project the workflow belongs to when there are several" in {
    val wid = seedWorkflow(ownerUid)
    val pid1 = resource.createProject(session(owner), "alpha").getPid
    val pid2 = resource.createProject(session(owner), "beta").getPid
    workflowOfProjectDao.insert(new WorkflowOfProject(wid, pid1))
    workflowOfProjectDao.insert(new WorkflowOfProject(wid, pid2))

    val status = ProjectResource.addExportedFileToProject(Integer.valueOf(ownerUid), wid, "out.csv")

    status should startWith("and added to projects: ")
    status should include("alpha")
    status should include("beta")
  }
}
