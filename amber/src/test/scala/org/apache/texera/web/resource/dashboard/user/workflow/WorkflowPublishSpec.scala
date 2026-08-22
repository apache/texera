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
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW_USER_ACCESS
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowUserAccessDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, Workflow, WorkflowUserAccess}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.OffsetDateTime
import javax.ws.rs.{BadRequestException, ForbiddenException, NotFoundException}

/**
  * Covers the publish state a workflow can be in: following the author's latest content, as
  * publishing has always done, or holding a pinned copy of the version the author froze.
  *
  * Only the state itself is covered here. Nothing reads the pinned copy yet, so the assertions are
  * about what is stored; the read paths that serve it arrive with the code that consults it.
  */
class WorkflowPublishSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with Matchers
    with MockTexeraDB {

  private val exampleCreationTime = OffsetDateTime.parse("2025-01-01T00:00:00Z")

  private def makeUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(Integer.valueOf(uid))
    user.setName(name)
    user.setEmail(s"$name@example.com")
    user.setRole(UserRoleEnum.ADMIN)
    user.setComment("test")
    user.setAccountCreationTime(exampleCreationTime)
    user
  }

  /** The author. */
  private val owner = makeUser(1, "publish_owner")

  /** A stranger: no access of their own, so nothing about this workflow is theirs to change. */
  private val stranger = makeUser(2, "publish_stranger")

  private val ownerSession = new SessionUser(owner)
  private val strangerSession = new SessionUser(stranger)

  private val workflowResource = new WorkflowResource()

  private val publishedContent = """{"operators":[],"note":"content_as_published"}"""
  private val editedContent = """{"operators":[],"note":"content_only_a_draft"}"""

  private def workflowDao = new WorkflowDao(getDSLContext.configuration())

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(owner)
    userDao.insert(stranger)
  }

  override protected def afterAll(): Unit = shutdownDB()

  /** Creates a workflow owned by `owner` holding [[publishedContent]]. */
  private def createWorkflow(name: String): Integer = {
    val workflow = new Workflow()
    workflow.setName(name)
    workflow.setDescription("a workflow")
    workflow.setContent(publishedContent)
    workflowResource.createWorkflow(workflow, ownerSession).workflow.getWid
  }

  /**
    * Publishes and pins in one step, which is the state most of these tests are about. Publishing on
    * its own leaves the workflow following the author's latest; pinning is what freezes a copy.
    */
  private def publishPinned(wid: Integer): WorkflowPublishService.PublishStatus = {
    workflowResource.makePublic(wid, ownerSession)
    workflowResource.pinLatest(wid, ownerSession)
  }

  /** Saves `content` as the author's working copy, the way an autosave would. */
  private def edit(wid: Integer, content: String): Unit = {
    val workflow = workflowDao.fetchOneByWid(wid)
    workflow.setContent(content)
    workflowResource.persistWorkflow(workflow, ownerSession)
  }

  private def statusOf(wid: Integer): WorkflowPublishService.PublishStatus =
    workflowResource.getPublishStatus(wid, ownerSession)

  /** Grants `stranger` explicit access, which makes them a collaborator rather than an outsider. */
  private def grantAccess(wid: Integer, privilege: PrivilegeEnum): Unit =
    new WorkflowUserAccessDao(getDSLContext.configuration())
      .insert(new WorkflowUserAccess(stranger.getUid, wid, privilege))

  private def revokeAccess(wid: Integer): Unit =
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.eq(stranger.getUid)))
      .execute()

  behavior of "publishing"

  it should "follow the author's latest by default" in {
    val wid = createWorkflow("publish_follows_latest")
    workflowResource.makePublic(wid, ownerSession)

    val status = statusOf(wid)
    status.isPublished shouldBe true
    status.isPinned shouldBe false
    // Nothing is frozen, so nothing is held back however much the author edits.
    status.hasUnpublishedChanges shouldBe false
    workflowDao.fetchOneByWid(wid).getPublishedContent shouldBe null

    edit(wid, editedContent)
    statusOf(wid).hasUnpublishedChanges shouldBe false
  }

  it should "pin the current version as the public copy" in {
    val wid = createWorkflow("pins_current_version")
    val status = publishPinned(wid)

    status.isPublished shouldBe true
    status.isPinned shouldBe true
    status.hasUnpublishedChanges shouldBe false
    workflowDao.fetchOneByWid(wid).getPublishedContent shouldBe publishedContent
  }

  it should "follow the author's latest again once the pin is dropped" in {
    val wid = createWorkflow("unpin_follows_latest")
    publishPinned(wid)
    edit(wid, editedContent)

    val status = workflowResource.unpin(wid, ownerSession)

    status.isPublished shouldBe true
    status.isPinned shouldBe false
    status.hasUnpublishedChanges shouldBe false
    // Still public; only the frozen copy is gone.
    val stored = workflowDao.fetchOneByWid(wid)
    stored.getIsPublic shouldBe true
    stored.getPublishedContent shouldBe null
  }

  it should "leave the pinned copy untouched when the author edits afterwards" in {
    val wid = createWorkflow("edit_stays_private")
    publishPinned(wid)

    edit(wid, editedContent)

    val stored = workflowDao.fetchOneByWid(wid)
    // The author's own working copy has moved on...
    stored.getContent shouldBe editedContent
    // ...but the copy that was frozen has not.
    stored.getPublishedContent shouldBe publishedContent
    statusOf(wid).hasUnpublishedChanges shouldBe true
  }

  it should "move the pin forward to the author's current version" in {
    val wid = createWorkflow("repin_updates_public")
    publishPinned(wid)
    edit(wid, editedContent)

    val status = workflowResource.pinLatest(wid, ownerSession)

    status.isPinned shouldBe true
    status.hasUnpublishedChanges shouldBe false
    workflowDao.fetchOneByWid(wid).getPublishedContent shouldBe editedContent
  }

  it should "report no unpublished changes when an edit is undone" in {
    val wid = createWorkflow("undo_clears_badge")
    publishPinned(wid)

    edit(wid, editedContent)
    statusOf(wid).hasUnpublishedChanges shouldBe true

    edit(wid, publishedContent)
    statusOf(wid).hasUnpublishedChanges shouldBe false
  }

  it should "report no unpublished changes when the same graph comes back rearranged" in {
    // The two copies travel by different routes, and the editor is free to hand back the same graph
    // with its keys in another order. Reporting that as an edit is an alarm the author cannot clear.
    val wid = createWorkflow("reformat_is_not_an_edit")
    publishPinned(wid)

    edit(wid, """{ "note":"content_as_published",  "operators": [] }""")

    statusOf(wid).hasUnpublishedChanges shouldBe false
  }

  it should "drop the pinned copy on unpublish" in {
    val wid = createWorkflow("unpublish_clears_pin")
    publishPinned(wid)

    workflowResource.makePrivate(wid, ownerSession)

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getIsPublic shouldBe false
    stored.getPublishedContent shouldBe null
  }

  it should "not resurrect the previous pin after unpublish and re-publish" in {
    val wid = createWorkflow("unpublish_then_publish")
    publishPinned(wid)
    edit(wid, editedContent)
    workflowResource.makePrivate(wid, ownerSession)

    // Publishing again starts in the following state; the copy that used to be public is gone.
    workflowResource.makePublic(wid, ownerSession)

    statusOf(wid).isPinned shouldBe false
    workflowDao.fetchOneByWid(wid).getPublishedContent shouldBe null
  }

  it should "publish a workflow that is created already public" in {
    val workflow = new Workflow()
    workflow.setName("created_public")
    workflow.setDescription("a workflow")
    workflow.setContent(publishedContent)
    workflow.setIsPublic(true)
    val wid = workflowResource.createWorkflow(workflow, ownerSession).workflow.getWid

    // Asking for a public workflow up front lands in the same following state as any other new
    // public workflow, rather than being pinned by surprise.
    val stored = workflowDao.fetchOneByWid(wid)
    stored.getIsPublic shouldBe true
    stored.getPublishedContent shouldBe null
  }

  it should "ignore publish columns supplied by the client on create" in {
    val workflow = new Workflow()
    workflow.setName("create_cannot_inject")
    workflow.setDescription("a workflow")
    workflow.setContent(publishedContent)
    workflow.setIsPublic(false)
    // A client cannot hand us a public copy of its own choosing, in any of its parts.
    workflow.setPublishedContent("""{"operators":[],"note":"injected"}""")
    workflow.setPublishedName("injected_name")
    workflow.setPublishedDescription("injected_description")
    workflow.setPublishedVersionId(1)

    val wid = workflowResource.createWorkflow(workflow, ownerSession).workflow.getWid

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getPublishedContent shouldBe null
    stored.getPublishedName shouldBe null
    stored.getPublishedDescription shouldBe null
    stored.getPublishedVersionId shouldBe null
  }

  it should "reject publishing by a user without write access" in {
    val wid = createWorkflow("publish_requires_write")
    a[ForbiddenException] should be thrownBy workflowResource.makePublic(wid, strangerSession)
  }

  it should "reject pinning and unpinning by a user without write access" in {
    val wid = createWorkflow("pin_requires_write")
    publishPinned(wid)
    a[ForbiddenException] should be thrownBy workflowResource.pinLatest(wid, strangerSession)
    a[ForbiddenException] should be thrownBy workflowResource.unpin(wid, strangerSession)
    a[ForbiddenException] should be thrownBy workflowResource.getPublishStatus(wid, strangerSession)
  }

  it should "reject pinning and unpinning a workflow that is not published" in {
    val wid = createWorkflow("pin_requires_published")
    a[BadRequestException] should be thrownBy workflowResource.pinLatest(wid, ownerSession)
    a[BadRequestException] should be thrownBy workflowResource.unpin(wid, ownerSession)
  }

  it should "reject publishing or pinning a workflow that does not exist" in {
    val missing = Integer.valueOf(987654)
    a[NotFoundException] should be thrownBy WorkflowPublishService.publish(missing)
    a[NotFoundException] should be thrownBy WorkflowPublishService.pinLatest(missing)
    a[NotFoundException] should be thrownBy WorkflowPublishService.unpin(missing)
    a[NotFoundException] should be thrownBy WorkflowPublishService.statusOf(missing)
  }

  behavior of "saving a published workflow"

  it should "not roll back a publish that lands while a save is in flight" in {
    // A save used to read every column and write them all back. A pin landing in that window was
    // silently reverted to whatever the save had read. The save statement no longer names the
    // publish columns at all, so the sequence is harmless.
    val wid = createWorkflow("save_cannot_roll_back_publish")
    publishPinned(wid)

    // A save built from a snapshot taken *before* a pin that happens in between.
    val stale = workflowDao.fetchOneByWid(wid)
    edit(wid, editedContent)
    workflowResource.pinLatest(wid, ownerSession)

    stale.setContent("""{"operators":[],"note":"from_a_stale_client"}""")
    workflowResource.persistWorkflow(stale, ownerSession)

    // The pin stands; only the working copy moved.
    workflowDao.fetchOneByWid(wid).getPublishedContent shouldBe editedContent
  }

  it should "not let a save change the publish state" in {
    val wid = createWorkflow("save_cannot_publish")
    publishPinned(wid)

    // A stale or hostile client sending the whole POJO back with the publish columns rewritten.
    val tampered = workflowDao.fetchOneByWid(wid)
    tampered.setContent(editedContent)
    tampered.setIsPublic(false)
    tampered.setPublishedContent(editedContent)
    workflowResource.persistWorkflow(tampered, ownerSession)

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getIsPublic shouldBe true
    stored.getPublishedContent shouldBe publishedContent
  }

  it should "not let a collaborator's save change the publish state" in {
    val wid = createWorkflow("collaborator_cannot_publish")
    publishPinned(wid)
    grantAccess(wid, PrivilegeEnum.WRITE)

    try {
      val tampered = workflowDao.fetchOneByWid(wid)
      tampered.setContent(editedContent)
      tampered.setIsPublic(false)
      tampered.setPublishedContent(editedContent)
      workflowResource.persistWorkflow(tampered, strangerSession)

      val stored = workflowDao.fetchOneByWid(wid)
      stored.getIsPublic shouldBe true
      stored.getPublishedContent shouldBe publishedContent
    } finally revokeAccess(wid)
  }

  it should "not let a rename change the publish state" in {
    val wid = createWorkflow("rename_cannot_publish")
    publishPinned(wid)

    val tampered = workflowDao.fetchOneByWid(wid)
    tampered.setName("renamed")
    tampered.setIsPublic(false)
    tampered.setPublishedContent(editedContent)
    workflowResource.updateWorkflowName(tampered, ownerSession)

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getName shouldBe "renamed"
    stored.getIsPublic shouldBe true
    stored.getPublishedContent shouldBe publishedContent
  }
}
