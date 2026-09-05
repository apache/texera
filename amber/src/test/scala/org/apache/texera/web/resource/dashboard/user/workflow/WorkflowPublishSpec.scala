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
import org.apache.texera.dao.jooq.generated.Tables.{WORKFLOW, WORKFLOW_USER_ACCESS}
import org.apache.texera.dao.jooq.generated.enums.{DefaultViewEnum, PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowUserAccessDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, Workflow, WorkflowUserAccess}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.jooq.{ExecuteContext, ExecuteListener}
import org.jooq.impl.{DefaultConfiguration, DefaultExecuteListenerProvider}

import java.time.OffsetDateTime
import javax.ws.rs.{BadRequestException, ForbiddenException, NotFoundException}

/**
  * Covers the publish state a workflow can be in: following the author's latest content, as
  * publishing has always done, or holding a pinned copy of the version the author froze.
  *
  * Only the state itself is covered here: nothing serves the pinned copy to a reader yet, so the
  * assertions are about which copy each operation leaves stored.
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

  /** Renames and re-describes the author's working copy, the way the dashboard does. */
  private def relabel(wid: Integer, name: String, description: String): Unit = {
    val workflow = workflowDao.fetchOneByWid(wid)
    workflow.setName(name)
    workflow.setDescription(description)
    workflowResource.persistWorkflow(workflow, ownerSession)
  }

  /**
    * Runs `interleaved` in the last moment before `act` sends its own write, which is where a second
    * request slips in unnoticed. Driven off the statement itself rather than off a thread, so the
    * ordering is the same on every run.
    */
  private def interleaving(interleaved: () => Unit)(act: => Unit): Unit = {
    var pending = true
    val configuration = getDSLContext.configuration().asInstanceOf[DefaultConfiguration]
    val previousListeners = configuration.executeListenerProviders()
    configuration.set(new DefaultExecuteListenerProvider(new ExecuteListener {
      override def executeStart(ctx: ExecuteContext): Unit = {
        // The workflow table itself, not workflow_version or the access tables: matching those too
        // would let a later change to one of these paths interleave at the wrong moment and leave
        // the test passing for the wrong reason.
        val sql = Option(ctx.sql()).getOrElse("").toLowerCase
        if (pending && sql.startsWith("update") && sql.contains("\"workflow\" set")) {
          pending = false
          interleaved()
        }
      }
    }))
    try act
    finally configuration.set(previousListeners: _*)
    withClue("nothing was interleaved, so this proves nothing: ") { pending shouldBe false }
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

  it should "pin a workflow that has no description" in {
    // description is nullable and the constraint does not ask for published_description, so a
    // workflow saved without one has to pin like any other rather than fail on the way in.
    val workflow = new Workflow()
    workflow.setName("pins_without_a_description")
    workflow.setContent(publishedContent)
    val wid = workflowResource.createWorkflow(workflow, ownerSession).workflow.getWid

    val status = publishPinned(wid)

    status.isPinned shouldBe true
    status.hasUnpublishedChanges shouldBe false
    val stored = workflowDao.fetchOneByWid(wid)
    stored.getDescription shouldBe null
    stored.getPublishedDescription shouldBe null

    // ...and writing one afterwards is an unpublished change like any other.
    relabel(wid, "pins_without_a_description", "described later")
    statusOf(wid).hasUnpublishedChanges shouldBe true
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

  it should "freeze the default view with the copy" in {
    // The form's definition rides inside the content, so pinning a canvas version and then switching
    // the workflow to the form view would otherwise leave the public opening a form that the frozen
    // copy does not contain.
    val wid = createWorkflow("view_freezes_with_the_copy")
    publishPinned(wid)

    getDSLContext
      .update(WORKFLOW)
      .set(WORKFLOW.DEFAULT_VIEW, DefaultViewEnum.FORM)
      .where(WORKFLOW.WID.eq(wid))
      .execute()

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getDefaultView shouldBe DefaultViewEnum.FORM
    stored.getPublishedDefaultView shouldBe DefaultViewEnum.CANVAS
  }

  it should "clear the frozen default view when the pin is dropped" in {
    val wid = createWorkflow("view_clears_with_the_pin")
    publishPinned(wid)
    workflowDao.fetchOneByWid(wid).getPublishedDefaultView shouldBe DefaultViewEnum.CANVAS

    workflowResource.unpin(wid, ownerSession)

    workflowDao.fetchOneByWid(wid).getPublishedDefaultView shouldBe null
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

  it should "pin the save that lands while it is pinning, not the version before it" in {
    // A pin that read the row and wrote what it had read would freeze the version before a save
    // landing in that window -- and the author, who had just pinned, would be told they have
    // unpublished changes. Each column is copied from its own row instead, so there is no window.
    val wid = createWorkflow("pin_takes_the_row_as_it_stands")
    workflowResource.makePublic(wid, ownerSession)

    interleaving(() => edit(wid, editedContent)) {
      workflowResource.pinLatest(wid, ownerSession)
    }

    workflowDao.fetchOneByWid(wid).getPublishedContent shouldBe editedContent
    statusOf(wid).hasUnpublishedChanges shouldBe false
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

  it should "count a rename as an unpublished change" in {
    // The pin freezes the title too, so the public is still being shown the old one -- the panel has
    // to say so, or the author reads "nothing held back" while the hub disagrees with their editor.
    val wid = createWorkflow("rename_counts_as_drift")
    publishPinned(wid)
    statusOf(wid).hasUnpublishedChanges shouldBe false

    relabel(wid, "renamed_after_pinning", "a workflow")

    statusOf(wid).hasUnpublishedChanges shouldBe true
  }

  it should "count a description edit as an unpublished change" in {
    val wid = createWorkflow("description_counts_as_drift")
    publishPinned(wid)

    relabel(wid, "description_counts_as_drift", "rewritten after pinning")

    statusOf(wid).hasUnpublishedChanges shouldBe true
  }

  it should "count a change of view as an unpublished change" in {
    val wid = createWorkflow("view_counts_as_drift")
    publishPinned(wid)

    getDSLContext
      .update(WORKFLOW)
      .set(WORKFLOW.DEFAULT_VIEW, DefaultViewEnum.FORM)
      .where(WORKFLOW.WID.eq(wid))
      .execute()

    statusOf(wid).hasUnpublishedChanges shouldBe true
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

  it should "do nothing when unpinning a workflow that is following" in {
    // The endpoint is reachable whatever the dialog shows, and asking for the state it is already in
    // is not an error -- it just has nothing to clear.
    val wid = createWorkflow("unpin_while_following")
    workflowResource.makePublic(wid, ownerSession)

    val status = workflowResource.unpin(wid, ownerSession)

    status.isPublished shouldBe true
    status.isPinned shouldBe false
    workflowDao.fetchOneByWid(wid).getIsPublic shouldBe true
  }

  it should "leave a pin alone when the workflow is published again" in {
    // Publishing is an on/off switch and this one is already on, so it has nothing to turn: the
    // frozen copy is not quietly dropped underneath a public that is reading it.
    val wid = createWorkflow("republish_keeps_the_pin")
    publishPinned(wid)

    workflowResource.makePublic(wid, ownerSession)

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getIsPublic shouldBe true
    stored.getPublishedContent shouldBe publishedContent
    statusOf(wid).isPinned shouldBe true
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

  it should "reject publishing by a user without write access" in {
    val wid = createWorkflow("publish_requires_write")
    a[ForbiddenException] should be thrownBy workflowResource.makePublic(wid, strangerSession)
  }

  it should "refuse to pin, unpin or report status without write access" in {
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

  it should "answer 404 for every operation on a workflow that does not exist" in {
    // Asked of the service rather than the endpoints: a missing workflow has no access row either,
    // so the endpoints answer 403 first and never reach these. 404 is the service's own contract.
    val missing = Integer.valueOf(987654)
    a[NotFoundException] should be thrownBy WorkflowPublishService.publish(missing)
    a[NotFoundException] should be thrownBy WorkflowPublishService.pinLatest(missing)
    a[NotFoundException] should be thrownBy WorkflowPublishService.unpin(missing)
    a[NotFoundException] should be thrownBy WorkflowPublishService.unpublish(missing)
    a[NotFoundException] should be thrownBy WorkflowPublishService.statusOf(missing)
  }

  behavior of "saving a published workflow"

  it should "not roll back a publish that lands while a save is in flight" in {
    // A save used to carry `is_public` along. An editor open since before the workflow was
    // published holds a snapshot saying private, and saving it put that back -- taking a pinned
    // workflow private underneath its own frozen copy, which the database refuses outright, so the
    // author was left with an editor that could no longer save. The save no longer names the column.
    val wid = createWorkflow("save_cannot_roll_back_publish")

    // The snapshot an editor opened before any of this was published.
    val stale = workflowDao.fetchOneByWid(wid)
    stale.getIsPublic shouldBe false

    publishPinned(wid)

    stale.setContent("""{"operators":[],"note":"from_a_stale_client"}""")
    workflowResource.persistWorkflow(stale, ownerSession)

    // The save went through, and it moved the working copy only.
    val stored = workflowDao.fetchOneByWid(wid)
    stored.getContent shouldBe """{"operators":[],"note":"from_a_stale_client"}"""
    stored.getIsPublic shouldBe true
    stored.getPublishedContent shouldBe publishedContent
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

  it should "not let a rename undo a publish that lands first" in {
    // A rename used to read the whole row and write it all back, so a publish landing in that window
    // was reverted to what the read had seen: the author pressed Public, was told it worked, and the
    // workflow was private again.
    val wid = createWorkflow("rename_cannot_undo_publish")

    interleaving(() => publishPinned(wid)) {
      val body = new Workflow()
      body.setWid(wid)
      body.setName("renamed_during_a_publish")
      workflowResource.updateWorkflowName(body, ownerSession)
    }

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getName shouldBe "renamed_during_a_publish"
    stored.getIsPublic shouldBe true
    stored.getPublishedContent shouldBe publishedContent
  }

  it should "not let a rename put an unpublished workflow back on show" in {
    // The same window, the other way round, and the one that matters: the author takes the workflow
    // down, and a rename in flight restores the row as it was -- public, still carrying the frozen
    // copy the public had been reading.
    val wid = createWorkflow("rename_cannot_republish")
    publishPinned(wid)

    interleaving(() => workflowResource.makePrivate(wid, ownerSession)) {
      val body = new Workflow()
      body.setWid(wid)
      body.setName("renamed_during_an_unpublish")
      workflowResource.updateWorkflowName(body, ownerSession)
    }

    val stored = workflowDao.fetchOneByWid(wid)
    stored.getName shouldBe "renamed_during_an_unpublish"
    stored.getIsPublic shouldBe false
    stored.getPublishedContent shouldBe null
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
