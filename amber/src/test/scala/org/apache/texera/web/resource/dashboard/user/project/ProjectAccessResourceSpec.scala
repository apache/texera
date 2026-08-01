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
import org.apache.texera.dao.jooq.generated.Tables.{PROJECT, PROJECT_USER_ACCESS, USER}
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{ProjectUserAccessDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{ProjectUserAccess, User}
import org.apache.texera.web.model.common.AccessEntry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import javax.ws.rs.ForbiddenException
import scala.jdk.CollectionConverters._

class ProjectAccessResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val ownerUid = 7101
  private val readerUid = 7102
  private val writerUid = 7103

  private var owner: User = _
  private var reader: User = _
  private var writer: User = _
  private var userDao: UserDao = _
  private var projectUserAccessDao: ProjectUserAccessDao = _
  private var projectResource: ProjectResource = _
  private var projectAccessResource: ProjectAccessResource = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    projectUserAccessDao = new ProjectUserAccessDao(getDSLContext.configuration())
    projectResource = new ProjectResource()
    projectAccessResource = new ProjectAccessResource()

    owner = createUser(ownerUid, "project_owner", "project-owner@test.com")
    reader = createUser(readerUid, "project_reader", "project-reader@test.com")
    writer = createUser(writerUid, "project_writer", "project-writer@test.com")

    cleanupTestData()

    userDao.insert(owner)
    userDao.insert(reader)
    userDao.insert(writer)
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

  private def createUser(uid: Int, name: String, email: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(email)
    user.setPassword("password")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private def cleanupTestData(): Unit = {
    getDSLContext
      .deleteFrom(PROJECT_USER_ACCESS)
      .where(PROJECT_USER_ACCESS.UID.in(ownerUid, readerUid, writerUid))
      .execute()

    getDSLContext
      .deleteFrom(PROJECT)
      .where(PROJECT.OWNER_ID.eq(ownerUid))
      .execute()

    getDSLContext
      .deleteFrom(USER)
      .where(USER.UID.in(ownerUid, readerUid, writerUid))
      .execute()
  }

  "ProjectAccessResource.getProjectAccessPrivilege" should "return WRITE if granted" in {
    val project = projectResource.createProject(new SessionUser(owner), "write-project")
    val privilege = ProjectAccessResource.getProjectAccessPrivilege(project.getPid, ownerUid)

    assert(privilege == PrivilegeEnum.WRITE)
    assert(ProjectAccessResource.userHasWriteAccess(project.getPid, ownerUid))
  }

  it should "return READ if a project access row grants READ" in {
    val project = projectResource.createProject(new SessionUser(owner), "read-project")
    projectUserAccessDao.merge(
      new ProjectUserAccess(readerUid, project.getPid, PrivilegeEnum.READ)
    )

    val privilege = ProjectAccessResource.getProjectAccessPrivilege(project.getPid, readerUid)

    assert(privilege == PrivilegeEnum.READ)
    assert(!ProjectAccessResource.userHasWriteAccess(project.getPid, readerUid))
  }

  it should "return NONE if the user only has access to another project" in {
    val sharedProject = projectResource.createProject(new SessionUser(owner), "shared-project")
    val privateProject = projectResource.createProject(new SessionUser(owner), "private-project")
    projectUserAccessDao.merge(
      new ProjectUserAccess(readerUid, sharedProject.getPid, PrivilegeEnum.READ)
    )

    val privilege =
      ProjectAccessResource.getProjectAccessPrivilege(privateProject.getPid, readerUid)

    assert(privilege == PrivilegeEnum.NONE)
    assert(!ProjectAccessResource.userHasWriteAccess(privateProject.getPid, readerUid))
  }

  it should "return WRITE and grant write access for a WRITE grantee" in {
    val project = projectResource.createProject(new SessionUser(owner), "writer-project")
    projectUserAccessDao.merge(
      new ProjectUserAccess(writerUid, project.getPid, PrivilegeEnum.WRITE)
    )

    assert(
      ProjectAccessResource.getProjectAccessPrivilege(
        project.getPid,
        writerUid
      ) == PrivilegeEnum.WRITE
    )
    assert(ProjectAccessResource.userHasWriteAccess(project.getPid, writerUid))
  }

  "ProjectAccessResource.getOwner" should "return the owning user's email" in {
    val project = projectResource.createProject(new SessionUser(owner), "owned-project")
    assert(projectAccessResource.getOwner(project.getPid) == owner.getEmail)
  }

  "ProjectAccessResource.getAccessList" should "be empty when only the owner has access" in {
    val project = projectResource.createProject(new SessionUser(owner), "solo-project")
    // createProject grants the owner WRITE, but getAccessList excludes the owner.
    assert(projectAccessResource.getAccessList(project.getPid).asScala.isEmpty)
  }

  it should "list every grantee (excluding the owner) with their email, name and privilege" in {
    val project = projectResource.createProject(new SessionUser(owner), "shared-list-project")
    projectUserAccessDao.merge(new ProjectUserAccess(readerUid, project.getPid, PrivilegeEnum.READ))
    projectUserAccessDao.merge(
      new ProjectUserAccess(writerUid, project.getPid, PrivilegeEnum.WRITE)
    )

    val entries = projectAccessResource.getAccessList(project.getPid).asScala.toList
    assert(entries.size == 2)
    assert(!entries.map(_.email).contains(owner.getEmail)) // owner is excluded
    assert(entries.contains(AccessEntry(reader.getEmail, reader.getName, PrivilegeEnum.READ)))
    assert(entries.contains(AccessEntry(writer.getEmail, writer.getName, PrivilegeEnum.WRITE)))
  }

  "ProjectAccessResource.grantAccess" should "let a WRITE grantee grant READ access to another user" in {
    val project = projectResource.createProject(new SessionUser(owner), "grant-project")
    // writer is a non-owner WRITE grantee, so it is allowed to grant access.
    projectUserAccessDao.merge(
      new ProjectUserAccess(writerUid, project.getPid, PrivilegeEnum.WRITE)
    )

    projectAccessResource.grantAccess(
      project.getPid,
      reader.getEmail,
      "READ",
      new SessionUser(writer)
    )

    assert(
      ProjectAccessResource.getProjectAccessPrivilege(
        project.getPid,
        readerUid
      ) == PrivilegeEnum.READ
    )
  }

  it should "reject a user without write access with ForbiddenException" in {
    val project = projectResource.createProject(new SessionUser(owner), "grant-forbidden-project")
    // reader has no access to the project, so cannot grant.
    assertThrows[ForbiddenException](
      projectAccessResource.grantAccess(
        project.getPid,
        writer.getEmail,
        "READ",
        new SessionUser(reader)
      )
    )
  }

  "ProjectAccessResource.revokeAccess" should "remove a grantee's access" in {
    val project = projectResource.createProject(new SessionUser(owner), "revoke-project")
    projectUserAccessDao.merge(new ProjectUserAccess(readerUid, project.getPid, PrivilegeEnum.READ))

    projectAccessResource.revokeAccess(project.getPid, reader.getEmail, new SessionUser(owner))

    assert(
      ProjectAccessResource.getProjectAccessPrivilege(
        project.getPid,
        readerUid
      ) == PrivilegeEnum.NONE
    )
  }

  it should "reject a user without write access with ForbiddenException" in {
    val project = projectResource.createProject(new SessionUser(owner), "revoke-forbidden-project")
    projectUserAccessDao.merge(
      new ProjectUserAccess(writerUid, project.getPid, PrivilegeEnum.WRITE)
    )

    // reader has no write access, so cannot revoke the writer's access.
    assertThrows[ForbiddenException](
      projectAccessResource.revokeAccess(project.getPid, writer.getEmail, new SessionUser(reader))
    )
  }
}
