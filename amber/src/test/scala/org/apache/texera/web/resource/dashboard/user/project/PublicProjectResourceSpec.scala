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
import org.apache.texera.dao.jooq.generated.Tables.{PROJECT, PROJECT_USER_ACCESS, PUBLIC_PROJECT}
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{ProjectDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Project, User}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.UUID
import scala.jdk.CollectionConverters._

class PublicProjectResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // MockTexeraDB gives each suite its own database, so a fixed uid is isolated and reproducible.
  private val testUid = 90001
  private var projectDao: ProjectDao = _
  private var sessionUser: SessionUser = _
  private val resource = new PublicProjectResource

  private def makeUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(
      s"public_project_spec_${uid}_${UUID.randomUUID().toString.substring(0, 8)}@example.com"
    )
    user.setPassword("password")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val user = makeUser(testUid, "public_project_owner")
    new UserDao(getDSLContext.configuration()).insert(user)
    sessionUser = new SessionUser(user)
    projectDao = new ProjectDao(getDSLContext.configuration())
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  // Remove the per-test project rows (children before parents) after each test; the owner
  // user seeded once in beforeAll is intentionally kept for the whole suite.
  override protected def afterEach(): Unit = {
    getDSLContext.deleteFrom(PUBLIC_PROJECT).where(PUBLIC_PROJECT.UID.eq(testUid)).execute()
    getDSLContext
      .deleteFrom(PROJECT_USER_ACCESS)
      .where(PROJECT_USER_ACCESS.UID.eq(testUid))
      .execute()
    getDSLContext.deleteFrom(PROJECT).where(PROJECT.OWNER_ID.eq(testUid)).execute()
  }

  // Insert a project owned by the test user; the generated pid is populated on the pojo.
  private def seedProject(name: String): Project = {
    val project = new Project(null, name, null, Integer.valueOf(testUid), null, null)
    projectDao.insert(project)
    project
  }

  "getType" should "report a project without a public flag as Private" in {
    val project = seedProject("p_private")
    resource.getType(project.getPid) shouldBe "Private"
  }

  "makePublic" should "flag a project public so getType reports Public" in {
    val project = seedProject("p_makepublic")
    resource.makePublic(project.getPid, sessionUser)
    resource.getType(project.getPid) shouldBe "Public"
  }

  "makePrivate" should "revert a public project so getType reports Private again" in {
    val project = seedProject("p_makeprivate")
    resource.makePublic(project.getPid, sessionUser)
    resource.getType(project.getPid) shouldBe "Public"

    resource.makePrivate(project.getPid)
    resource.getType(project.getPid) shouldBe "Private"
  }

  "listPublicProjects" should "return an empty list when no project is public" in {
    seedProject("p_still_private")
    resource.listPublicProjects().asScala shouldBe empty
  }

  it should "return only the currently-public projects with their name and owner" in {
    val publicProject = seedProject("p_public")
    seedProject("p_hidden")
    resource.makePublic(publicProject.getPid, sessionUser)

    val listed = resource.listPublicProjects().asScala
    listed.map(_.pid) shouldBe Seq(publicProject.getPid)
    listed.head.name shouldBe "p_public"
    listed.head.owner shouldBe "public_project_owner"
  }

  // NOTE: despite the name, addPublicProjects does NOT set the public flag — it grants the
  // caller READ access to each project (a ProjectUserAccess row) and leaves getType Private.
  "addPublicProjects" should "grant the caller READ access to each listed project" in {
    val p1 = seedProject("p_add1")
    val p2 = seedProject("p_add2")

    resource.addPublicProjects(util.Arrays.asList(p1.getPid, p2.getPid), sessionUser)

    val grantedPids = getDSLContext
      .select(PROJECT_USER_ACCESS.PID)
      .from(PROJECT_USER_ACCESS)
      .where(
        PROJECT_USER_ACCESS.UID
          .eq(testUid)
          .and(PROJECT_USER_ACCESS.PRIVILEGE.eq(PrivilegeEnum.READ))
      )
      .fetchInto(classOf[Integer])
      .asScala
    grantedPids should contain allOf (p1.getPid, p2.getPid)
    resource.getType(p1.getPid) shouldBe "Private" // access grant is not the public flag
  }
}
