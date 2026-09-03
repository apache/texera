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

package org.apache.texera.web.resource.dashboard.user.workflow

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{DefaultViewEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, Workflow}
import org.jooq.exception.DataAccessException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.OffsetDateTime

/**
  * The columns a pinned public copy lives in, and the constraint that keeps them honest.
  *
  * Nothing pins anything yet, so this covers what the schema alone guarantees: a workflow public
  * today keeps behaving as it does, a private one can never carry part of a frozen copy, and the two
  * endpoints that take a whole Workflow from the request body cannot seed one.
  */
class PublishedCopySchemaSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private var workflowDao: WorkflowDao = _
  private var userDao: UserDao = _

  /** A create needs an owner row to attach the workflow to. */
  private val owner = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("schema_owner")
    user.setEmail("schema_owner@example.com")
    user.setRole(UserRoleEnum.ADMIN)
    user.setComment("test")
    user.setAccountCreationTime(OffsetDateTime.parse("2025-01-01T00:00:00Z"))
    user
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(owner)
  }

  override protected def afterAll(): Unit = shutdownDB()

  /** A workflow POJO the resource layer can take, with nothing frozen on it. */
  private def newWorkflow(name: String, isPublic: Boolean): Workflow = {
    val workflow = new Workflow()
    workflow.setName(name)
    workflow.setDescription("a workflow")
    workflow.setContent("""{"operators":[]}""")
    workflow.setIsPublic(isPublic)
    workflow
  }

  /** The same workflow, stored: what a row looks like before anything pins it. */
  private def insertWorkflow(name: String, isPublic: Boolean): Workflow = {
    val workflow = newWorkflow(name, isPublic)
    workflowDao.insert(workflow)
    workflowDao.fetchOneByWid(workflow.getWid)
  }

  behavior of "the published-copy columns"

  it should "leave every workflow following the author's latest" in {
    // The migration adds columns and no backfill, so a workflow that was public before it ran shows
    // exactly what it showed: nothing is frozen, which is the state the rest of the feature calls
    // "following".
    val stored = insertWorkflow("migration_changes_nothing", isPublic = true)

    stored.getPublishedContent shouldBe null
    stored.getPublishedName shouldBe null
    stored.getPublishedDescription shouldBe null
    stored.getPublishedVersionId shouldBe null
    stored.getPublishedDefaultView shouldBe null
  }

  it should "let a public workflow carry a frozen copy" in {
    val stored = insertWorkflow("public_may_be_pinned", isPublic = true)
    stored.setPublishedContent("""{"operators":[]}""")
    stored.setPublishedName("frozen name")
    stored.setPublishedDescription("frozen description")
    stored.setPublishedDefaultView(DefaultViewEnum.CANVAS)

    workflowDao.update(stored)

    workflowDao.fetchOneByWid(stored.getWid).getPublishedName shouldBe "frozen name"
  }

  it should "ignore publish columns supplied by the client on create" in {
    // The columns are part of the generated POJO, and `POST /workflow/create` takes one whole. A
    // client must not be able to seed a published copy of its own choosing before anything can pin.
    val workflow = newWorkflow("create_cannot_inject", isPublic = false)
    workflow.setPublishedContent("""{"operators":[],"note":"injected"}""")
    workflow.setPublishedName("injected_name")
    workflow.setPublishedDescription("injected_description")
    workflow.setPublishedVersionId(1)
    workflow.setPublishedDefaultView(DefaultViewEnum.FORM)

    val wid = new WorkflowResource()
      .createWorkflow(workflow, new SessionUser(owner))
      .workflow
      .getWid

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getPublishedContent shouldBe null
    stored.getPublishedName shouldBe null
    stored.getPublishedDescription shouldBe null
    stored.getPublishedVersionId shouldBe null
    stored.getPublishedDefaultView shouldBe null
  }

  it should "refuse a pinned copy with no default view" in {
    // The form's definition rides inside the frozen content, so a pin that did not carry the view
    // would leave the public opening a frozen graph under the author's live preference.
    val stored = insertWorkflow("pinned_needs_a_view", isPublic = true)
    stored.setPublishedContent("""{"operators":[]}""")
    stored.setPublishedName("frozen name")

    a[DataAccessException] should be thrownBy workflowDao.update(stored)
  }

  it should "refuse a pinned copy with no name" in {
    // A pinned copy is what the public sees, and a workflow always has a name, so a frozen copy
    // without one is a half-written pin rather than a legitimate state.
    val stored = insertWorkflow("pinned_needs_a_name", isPublic = true)
    stored.setPublishedContent("""{"operators":[]}""")

    a[DataAccessException] should be thrownBy workflowDao.update(stored)
  }

  it should "refuse a private workflow that carries any part of a frozen copy" in {
    // The five columns describe one copy, so they travel together: a path that cleared four of them
    // must not be able to leave the fifth behind on a private row.
    val leftovers = Seq[(String, Workflow => Unit)](
      "content" -> (_.setPublishedContent("""{"operators":[]}""")),
      "name" -> (_.setPublishedName("frozen name")),
      "description" -> (_.setPublishedDescription("frozen description")),
      "version" -> (_.setPublishedVersionId(1)),
      "view" -> (_.setPublishedDefaultView(DefaultViewEnum.FORM))
    )
    for ((column, leaveBehind) <- leftovers) {
      val stored = insertWorkflow(s"private_cannot_keep_$column", isPublic = false)
      leaveBehind(stored)
      withClue(s"a private workflow kept published_$column: ") {
        a[DataAccessException] should be thrownBy workflowDao.update(stored)
      }
    }
  }

  it should "ignore publish columns supplied by the client on save" in {
    // `/workflow/persist` is the autosave path and takes a whole Workflow from the request body. If
    // a save could write these columns, a client could pin a copy of its own choosing on a public
    // workflow -- and unpublishing it afterwards would then fail the constraint, leaving a workflow
    // that can never be made private again.
    val wid = new WorkflowResource()
      .createWorkflow(newWorkflow("save_cannot_inject", isPublic = true), new SessionUser(owner))
      .workflow
      .getWid

    val tampered = workflowDao.fetchOneByWid(wid)
    tampered.setPublishedContent("""{"operators":[],"note":"injected"}""")
    tampered.setPublishedName("injected_name")
    tampered.setPublishedDescription("injected_description")
    tampered.setPublishedVersionId(1)
    tampered.setPublishedDefaultView(DefaultViewEnum.FORM)
    new WorkflowResource().persistWorkflow(tampered, new SessionUser(owner))

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getPublishedContent shouldBe null
    stored.getPublishedName shouldBe null
    stored.getPublishedDescription shouldBe null
    stored.getPublishedVersionId shouldBe null
    stored.getPublishedDefaultView shouldBe null
  }

}
