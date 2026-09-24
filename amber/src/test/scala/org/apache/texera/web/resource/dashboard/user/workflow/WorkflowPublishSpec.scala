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
import org.apache.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.apache.texera.web.resource.dashboard.hub.HubResource
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowResource.WorkflowIDs
import org.apache.texera.web.resource.dashboard.{DashboardResource, FulltextSearchQueryUtils}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.jooq.{ExecuteContext, ExecuteListener}
import org.jooq.impl.{DefaultConfiguration, DefaultExecuteListenerProvider}
import java.lang.reflect.Proxy
import java.time.OffsetDateTime
import java.util
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.{BadRequestException, ForbiddenException, NotFoundException}

/**
  * Covers the publish state a workflow can be in -- following the author's latest content, as
  * publishing has always done, or holding a pinned copy of the version the author froze -- and the
  * read paths that decide which of the two a caller is served, listings and search among them.
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
  private val versionResource = new WorkflowVersionResource()

  private val publishedContent = """{"operators":[],"note":"content_as_published"}"""
  private val editedContent = """{"operators":[],"note":"content_only_a_draft"}"""

  private def workflowDao = new WorkflowDao(getDSLContext.configuration())

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    FulltextSearchQueryUtils.usePgroonga = false
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
  /**
    * An UPDATE of the workflow table itself, whatever quoting and spacing jOOQ renders it with --
    * and not one of workflow_version or the access tables, since matching those would let a later
    * change to one of these paths interleave at the wrong moment and leave the test passing for the
    * wrong reason. A rendering this fails to recognise turns the test red rather than skipping it,
    * because [[interleaving]] insists that something was interleaved.
    */
  private val aWorkflowUpdate =
    """(?is)\s*update\s+(?:\S*\.)?["`\[]?workflow["`\]]?\s+set\b.*""".r

  private def interleaving(interleaved: () => Unit)(act: => Unit): Unit = {
    var pending = true
    val configuration = getDSLContext.configuration().asInstanceOf[DefaultConfiguration]
    val previousListeners = configuration.executeListenerProviders()
    configuration.set(new DefaultExecuteListenerProvider(new ExecuteListener {
      override def executeStart(ctx: ExecuteContext): Unit = {
        val sql = Option(ctx.sql()).getOrElse("")
        if (pending && aWorkflowUpdate.matches(sql)) {
          pending = false
          interleaved()
        }
      }
    }))
    try act
    finally configuration.set(previousListeners: _*)
    withClue("nothing was interleaved, so this proves nothing: ") { pending shouldBe false }
  } /** Clone records the caller's IP, and that is the only thing it wants from the request.
    */
  private def fakeRequest(): HttpServletRequest =
    Proxy
      .newProxyInstance(
        classOf[HttpServletRequest].getClassLoader,
        Array[Class[_]](classOf[HttpServletRequest]),
        (_: Any, method: java.lang.reflect.Method, _: Array[AnyRef]) =>
          if (method.getName == "getRemoteAddr") "127.0.0.1" else null
      )
      .asInstanceOf[HttpServletRequest]

  private def keywords(values: String*): util.ArrayList[String] = {
    val list = new util.ArrayList[String]()
    values.foreach(list.add)
    list
  }

  private def keywordIds(values: Integer*): util.ArrayList[Integer] = {
    val list = new util.ArrayList[Integer]()
    values.foreach(list.add)
    list
  }

  /** The wids a search returns, which is all any of these tests asks of one. */
  private def searchWids(user: SessionUser, params: SearchQueryParams): List[Integer] =
    DashboardResource
      .searchAllResources(user, params, includePublic = true)
      .results
      .flatMap(_.workflow.map(_.workflow.getWid))

  /** One workflow's listing row, as the given user would see it in a dashboard or on the Hub. */
  private def listingOf(user: SessionUser, wid: Integer) =
    DashboardResource
      .searchAllResources(
        user,
        SearchQueryParams(workflowIDs = keywordIds(wid)),
        includePublic = true
      )
      .results
      .flatMap(_.workflow)
      .head

  /** Nobody: the hub as an unauthenticated visitor reads it. */
  private def anonymous: SessionUser = new SessionUser(new User())

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

  it should "report drift rather than fail when a copy is not valid JSON" in {
    // content is free text as far as the database is concerned, so the comparison has to survive a
    // blob it cannot parse. Falling back to "these differ" is the safe direction: the author is told
    // the public is behind, rather than the dialog throwing at them.
    val wid = createWorkflow("unparsable_content")
    publishPinned(wid)

    getDSLContext
      .update(WORKFLOW)
      .set(WORKFLOW.CONTENT, "not json at all")
      .where(WORKFLOW.WID.eq(wid))
      .execute()

    statusOf(wid).hasUnpublishedChanges shouldBe true
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

  behavior of "name and description"

  it should "freeze the name and description alongside the content" in {
    // Malicious text in a description is just as public as the graph, so editing it must not reach
    // the public view either -- otherwise a report can be answered by rewording rather than fixing.
    val wid = createWorkflow("freeze_metadata")
    publishPinned(wid)

    relabel(wid, "renamed_after_publishing", "rewritten after publishing")

    val publicView = workflowResource.retrievePublicWorkflow(wid)
    publicView.name shouldBe "freeze_metadata"
    publicView.description shouldBe "a workflow"
    workflowResource.getWorkflowName(wid) shouldBe "freeze_metadata"
    workflowResource.getWorkflowDescription(wid) shouldBe "a workflow"
  }

  it should "publish an edited description by moving the pin forward" in {
    val wid = createWorkflow("description_publishes_on_repin")
    publishPinned(wid)
    relabel(wid, "description_publishes_on_repin", "rewritten after publishing")

    workflowResource.pinLatest(wid, ownerSession)

    workflowResource.retrievePublicWorkflow(wid).description shouldBe "rewritten after publishing"
    statusOf(wid).hasUnpublishedChanges shouldBe false
  }

  behavior of "public read paths"

  it should "show a public viewer the author's latest while nothing is pinned" in {
    // Following follows everything a pin would freeze, not just the graph: name and description are
    // on public show too, so a viewer must see the live ones or the two halves would disagree.
    val wid = createWorkflow("following_serves_latest_to_strangers")
    workflowResource.makePublic(wid, ownerSession)
    edit(wid, editedContent)
    relabel(wid, "renamed_while_following", "described_while_following")

    workflowResource.retrieveWorkflow(wid, strangerSession).content shouldBe editedContent
    workflowResource.getWorkflowName(wid) shouldBe "renamed_while_following"
    workflowResource.getWorkflowDescription(wid) shouldBe "described_while_following"

    val publicView = workflowResource.retrievePublicWorkflow(wid)
    publicView.content shouldBe editedContent
    publicView.name shouldBe "renamed_while_following"
    publicView.description shouldBe "described_while_following"
  }

  it should "open the public view in the pinned copy's own view" in {
    // A form's definition lives inside the content, so serving the author's live preference over a
    // frozen graph would put a form on a copy that has none.
    val wid = createWorkflow("public_view_uses_the_frozen_view")
    publishPinned(wid)

    getDSLContext
      .update(WORKFLOW)
      .set(WORKFLOW.DEFAULT_VIEW, DefaultViewEnum.FORM)
      .where(WORKFLOW.WID.eq(wid))
      .execute()

    workflowResource.retrievePublicWorkflow(wid).defaultView shouldBe DefaultViewEnum.CANVAS
    // The author keeps their own preference.
    workflowDao.fetchOneByWid(wid).getDefaultView shouldBe DefaultViewEnum.FORM
  }

  it should "follow the author's view while nothing is pinned" in {
    val wid = createWorkflow("public_view_follows_latest")
    workflowResource.makePublic(wid, ownerSession)

    getDSLContext
      .update(WORKFLOW)
      .set(WORKFLOW.DEFAULT_VIEW, DefaultViewEnum.FORM)
      .where(WORKFLOW.WID.eq(wid))
      .execute()

    workflowResource.retrievePublicWorkflow(wid).defaultView shouldBe DefaultViewEnum.FORM
  }

  it should "serve the published version to a user without granted access" in {
    val wid = createWorkflow("read_serves_published")
    publishPinned(wid)
    edit(wid, editedContent)

    // A stranger reaches this workflow only because it is public.
    workflowResource.retrieveWorkflow(wid, strangerSession).content shouldBe publishedContent
    // The author keeps seeing their own working copy.
    workflowResource.retrieveWorkflow(wid, ownerSession).content shouldBe editedContent
  }

  it should "open a pinned workflow under the copy's own title and view, not the author's" in {
    // The graph and the label travel together: a public viewer opening this workflow must not get
    // the frozen graph under a title the author has not published, and must not be told to open a
    // form view over a copy whose content carries no form.
    val wid = createWorkflow("open_serves_the_whole_copy")
    publishPinned(wid)
    edit(wid, editedContent)
    relabel(wid, "renamed_after_pinning", "described_after_pinning")
    getDSLContext
      .update(WORKFLOW)
      .set(WORKFLOW.DEFAULT_VIEW, DefaultViewEnum.FORM)
      .where(WORKFLOW.WID.eq(wid))
      .execute()

    val asStranger = workflowResource.retrieveWorkflow(wid, strangerSession)
    asStranger.content shouldBe publishedContent
    asStranger.name shouldBe "open_serves_the_whole_copy"
    asStranger.description shouldBe "a workflow"
    asStranger.defaultView shouldBe DefaultViewEnum.CANVAS

    // The author opens their own workflow and sees everything they have.
    val asOwner = workflowResource.retrieveWorkflow(wid, ownerSession)
    asOwner.content shouldBe editedContent
    asOwner.name shouldBe "renamed_after_pinning"
    asOwner.defaultView shouldBe DefaultViewEnum.FORM
  }

  it should "serve the working copy to a collaborator with granted read access" in {
    val wid = createWorkflow("collaborator_sees_working_copy")
    publishPinned(wid)
    edit(wid, editedContent)
    grantAccess(wid, PrivilegeEnum.READ)

    try {
      // Sharing is not publishing: a collaborator tracks the author's latest content, live.
      workflowResource.retrieveWorkflow(wid, strangerSession).content shouldBe editedContent
    } finally revokeAccess(wid)
  }

  it should "keep serving a collaborator the latest content as the author keeps editing" in {
    val wid = createWorkflow("collaborator_tracks_latest")
    publishPinned(wid)
    grantAccess(wid, PrivilegeEnum.READ)

    try {
      val later = """{"operators":[],"note":"later_still"}"""
      edit(wid, editedContent)
      workflowResource.retrieveWorkflow(wid, strangerSession).content shouldBe editedContent
      edit(wid, later)
      workflowResource.retrieveWorkflow(wid, strangerSession).content shouldBe later
      // ...while the public copy stayed put throughout.
      workflowResource.retrievePublicWorkflow(wid).content shouldBe publishedContent
    } finally revokeAccess(wid)
  }

  it should "not tell a public viewer that the author has unpublished edits" in {
    val wid = createWorkflow("draft_state_is_private")
    publishPinned(wid)
    edit(wid, editedContent)

    a[ForbiddenException] should be thrownBy workflowResource.getPublishStatus(wid, strangerSession)
    statusOf(wid).hasUnpublishedChanges shouldBe true
  }

  it should "not expose the author's edit history to a public viewer" in {
    // The other way into the working copy. Replaying a version folds deltas back from the author's
    // *current* content, so a public viewer listing versions and checking one out is handed a draft
    // the pin is holding back -- the one thing the frozen copy exists to prevent.
    val wid = createWorkflow("history_is_not_public")
    publishPinned(wid)
    edit(wid, editedContent)
    edit(wid, """{"operators":[],"note":"newer_still"}""")

    val ownerVersions = versionResource.retrieveVersionsOfWorkflow(wid, ownerSession)
    ownerVersions should not be empty
    versionResource.retrieveVersionsOfWorkflow(wid, strangerSession) shouldBe empty
    a[ForbiddenException] should be thrownBy
      versionResource.retrieveWorkflowVersion(wid, ownerVersions.head.vId, strangerSession)
  }

  it should "still expose the history of a public workflow that is not pinned" in {
    // Nothing is frozen, so the public copy is the author's latest and its history is the history of
    // what everyone can already see. Taking that away would be a change this feature does not need.
    val wid = createWorkflow("history_stays_public_while_following")
    workflowResource.makePublic(wid, ownerSession)
    edit(wid, editedContent)

    val versions = versionResource.retrieveVersionsOfWorkflow(wid, strangerSession)
    versions should not be empty
    versionResource
      .retrieveWorkflowVersion(wid, versions.head.vId, strangerSession)
      .getContent should not be empty
  }

  it should "still expose the edit history to a collaborator" in {
    // Sharing is not publishing: someone granted access tracks the author's latest, history included.
    val wid = createWorkflow("history_visible_to_collaborator")
    publishPinned(wid)
    grantAccess(wid, PrivilegeEnum.READ)
    try {
      versionResource.retrieveVersionsOfWorkflow(wid, strangerSession) should not be empty
    } finally revokeAccess(wid)
  }

  it should "refuse to hand out a public copy of a workflow that is not public" in {
    // The guard viewers without granted access rely on: no route to a private workflow's content
    // may fall through to the public copy just because the caller asked for it by wid.
    val wid = createWorkflow("public_copy_requires_public")
    a[NotFoundException] should be thrownBy WorkflowPublishService.publicCopyOf(wid)
  }

  it should "clone the published version, not the author's latest" in {
    val wid = createWorkflow("clone_takes_published")
    publishPinned(wid)
    edit(wid, editedContent)

    val clonedWid = workflowResource.cloneWorkflow(wid, strangerSession, fakeRequest())
    val cloned = workflowDao.fetchOneByWid(clonedWid)

    cloned.getContent shouldBe publishedContent
    // A copy has never been reviewed, so it starts private.
    cloned.getIsPublic shouldBe false
  }

  it should "clone the published version for the author too" in {
    // The hub shows the pinned version, so its Clone button copies that even for the author, whose
    // working copy has moved on -- cloning something other than what is on the screen would be the
    // surprise, and their latest is already open in the editor.
    val wid = createWorkflow("clone_takes_published_for_author")
    publishPinned(wid)
    edit(wid, editedContent)

    val clonedWid = workflowResource.cloneWorkflow(wid, ownerSession, fakeRequest())

    workflowDao.fetchOneByWid(clonedWid).getContent shouldBe publishedContent
  }

  it should "clone the published name and description, not the edited ones" in {
    val wid = createWorkflow("clone_takes_published_metadata")
    publishPinned(wid)
    relabel(wid, "renamed_after_publishing", "described_after_publishing")

    val cloned =
      workflowDao.fetchOneByWid(workflowResource.cloneWorkflow(wid, ownerSession, fakeRequest()))

    cloned.getName shouldBe "clone_takes_published_metadata_clone"
    cloned.getDescription shouldBe "a workflow"
  }

  it should "still clone the working copy of a workflow that is not public" in {
    val wid = createWorkflow("clone_private_takes_working_copy")
    edit(wid, editedContent)

    val clonedWid = workflowResource.cloneWorkflow(wid, ownerSession, fakeRequest())

    workflowDao.fetchOneByWid(clonedWid).getContent shouldBe editedContent
  }

  it should "duplicate the published version for a user without granted access" in {
    // Title and description too, not just the graph: a copy carrying the published canvas under the
    // author's unpublished title would publish the very rename the pin is holding back.
    val wid = createWorkflow("duplicate_takes_published")
    publishPinned(wid)
    edit(wid, editedContent)
    relabel(wid, "duplicate_unpublished_name", "unpublished description")

    val duplicated =
      workflowResource.duplicateWorkflow(WorkflowIDs(List(wid)), strangerSession)

    duplicated should have size 1
    val copy = workflowDao.fetchOneByWid(duplicated.head.workflow.getWid)
    copy.getContent shouldBe publishedContent
    copy.getName shouldBe "duplicate_takes_published_copy"
    copy.getDescription should not be "unpublished description"
  }

  it should "duplicate the owner's own working copy for the owner" in {
    val wid = createWorkflow("owner_duplicates_working_copy")
    publishPinned(wid)
    edit(wid, editedContent)

    val duplicated = workflowResource.duplicateWorkflow(WorkflowIDs(List(wid)), ownerSession)
    workflowDao.fetchOneByWid(duplicated.head.workflow.getWid).getContent shouldBe editedContent
  }

  it should "start a copy of a published workflow with no publish state of its own" in {
    val wid = createWorkflow("copy_starts_clean")
    publishPinned(wid)

    val copy = workflowDao.fetchOneByWid(
      workflowResource
        .duplicateWorkflow(WorkflowIDs(List(wid)), ownerSession)
        .head
        .workflow
        .getWid
    )

    copy.getIsPublic shouldBe false
    copy.getPublishedContent shouldBe null
    copy.getPublishedName shouldBe null
    copy.getPublishedDescription shouldBe null
    copy.getPublishedDefaultView shouldBe null
    copy.getPublishedVersionId shouldBe null
  }

  it should "size a workflow by the copy the caller can see" in {
    // Listings show a size next to every card, so it has to describe the copy that card opens: the
    // pinned one while a pin is in place, the author's latest otherwise, and a private workflow's
    // own content.
    val priv = createWorkflow("private_size_uses_content")
    edit(priv, editedContent)
    workflowResource.getSize(util.Arrays.asList(priv)).get(priv) shouldBe editedContent.length

    val following = createWorkflow("size_follows_latest")
    workflowResource.makePublic(following, ownerSession)
    edit(following, editedContent + "     ")
    workflowResource
      .getSize(util.Arrays.asList(following))
      .get(following) shouldBe editedContent.length + 5

    val pinned = createWorkflow("size_uses_published")
    publishPinned(pinned)
    edit(pinned, editedContent + "                    ")
    workflowResource
      .getSize(util.Arrays.asList(pinned))
      .get(pinned) shouldBe publishedContent.length
  }

  behavior of "hub listings"

  it should "list a pinned workflow on the hub under the name and description it froze" in {
    // The hub is the public shelf: everything on it is listed as the public sees it, the author
    // included. A pin that this listing did not honour would put the author's live title on that
    // shelf, and would leave it disagreeing with the hub's own search about what a workflow is
    // called.
    val wid = createWorkflow("hub_listing_shows_public_copy")
    publishPinned(wid)
    relabel(wid, "renamed_after_pinning", "described_after_pinning")

    for (viewer <- Seq(stranger.getUid, owner.getUid)) {
      val listed = HubResource.fetchDashboardWorkflowsByWids(Seq(wid), viewer).head.workflow
      listed.getName shouldBe "hub_listing_shows_public_copy"
      listed.getDescription shouldBe "a workflow"
    }
  }

  it should "list a following workflow on the hub under the author's latest name and description" in {
    val wid = createWorkflow("hub_listing_follows_latest")
    workflowResource.makePublic(wid, ownerSession)
    relabel(wid, "renamed_while_following", "described_while_following")

    val listed = HubResource.fetchDashboardWorkflowsByWids(Seq(wid), owner.getUid).head.workflow
    listed.getName shouldBe "renamed_while_following"
    listed.getDescription shouldBe "described_while_following"
  }

  it should "tell the hub listing when the copy on show is behind the author's working copy" in {
    // The card advertises the pinned copy, so clicking it has to open that copy. Without this flag
    // the author's own card would take them to their editor and show them something else -- the
    // same signal the search listing carries, on the query the hub builds for itself.
    val wid = createWorkflow("hub_listing_reports_drift")
    publishPinned(wid)

    def listedDrift(): Boolean =
      HubResource.fetchDashboardWorkflowsByWids(Seq(wid), owner.getUid).head.hasUnpublishedChanges

    listedDrift() shouldBe false
    edit(wid, editedContent)
    listedDrift() shouldBe true
    workflowResource.pinLatest(wid, ownerSession)
    listedDrift() shouldBe false
  }

  it should "clone the author's latest name and description while nothing is pinned" in {
    val wid = createWorkflow("clone_follows_latest_metadata")
    workflowResource.makePublic(wid, ownerSession)
    relabel(wid, "renamed_before_clone", "described_before_clone")

    val cloned =
      workflowDao.fetchOneByWid(workflowResource.cloneWorkflow(wid, strangerSession, fakeRequest()))

    cloned.getName shouldBe "renamed_before_clone_clone"
    cloned.getDescription shouldBe "described_before_clone"
  }

  behavior of "search"

  it should "not match a public workflow on anything that exists only in unpublished edits" in {
    // Both halves of what public search indexes -- the words in the graph, and the operators in it --
    // have to stop at the pinned copy, or searching would surface drafts nobody can open.
    val wid = createWorkflow("search_ignores_drafts")
    publishPinned(wid)
    edit(
      wid,
      """{"operators":[{"operatorType":"SecretDraftOperator"}],"note":"supersecretdraftword"}"""
    )

    val byKeyword =
      searchWids(anonymous, SearchQueryParams(keywords = keywords("supersecretdraftword")))
    val byOperator =
      searchWids(anonymous, SearchQueryParams(operators = keywords("SecretDraftOperator")))

    byKeyword should not contain wid
    byOperator should not contain wid
  }

  it should "match a public workflow on keywords in its published copy" in {
    val wid = createWorkflow("search_finds_published")
    publishPinned(wid)
    edit(wid, editedContent)

    searchWids(
      anonymous,
      SearchQueryParams(keywords = keywords("content_as_published"))
    ) should contain(wid)
  }

  it should "match an unpinned public workflow on the author's latest" in {
    // Following means the public copy is the working copy, so search must reach it through the same
    // public path that a pinned workflow reaches its frozen copy through.
    val wid = createWorkflow("search_finds_unpinned_latest")
    workflowResource.makePublic(wid, ownerSession)
    edit(wid, """{"operators":[],"note":"unpinnedsearchword"}""")

    searchWids(
      anonymous,
      SearchQueryParams(keywords = keywords("unpinnedsearchword"))
    ) should contain(wid)
  }

  it should "still match the author's own workflow on their unpublished edits" in {
    val wid = createWorkflow("search_finds_own_draft")
    publishPinned(wid)
    edit(wid, """{"operators":[],"note":"myowndraftword"}""")

    searchWids(
      ownerSession,
      SearchQueryParams(keywords = keywords("myowndraftword"))
    ) should contain(wid)
  }

  it should "match a pinned workflow on the name and description the public can see" in {
    // The graph was already matched against the pinned copy; the title and description are on public
    // show just as much, so matching them against the author's live values gets it wrong in both
    // directions at once -- findable by a title nobody has seen, unfindable by the one on screen.
    val wid = createWorkflow("aapublicnamesearch")
    relabel(wid, "aapublicnamesearch", "aapublicdescriptionsearch")
    publishPinned(wid)
    relabel(wid, "zzsecretnamesearch", "zzsecretdescriptionsearch")

    def anonymousHits(word: String): List[Integer] =
      searchWids(anonymous, SearchQueryParams(keywords = keywords(word)))

    anonymousHits("aapublicnamesearch") should contain(wid)
    anonymousHits("aapublicdescriptionsearch") should contain(wid)
    anonymousHits("zzsecretnamesearch") should not contain wid
    anonymousHits("zzsecretdescriptionsearch") should not contain wid
  }

  it should "match an unpinned public workflow on its live name and description" in {
    val wid = createWorkflow("bbfollowingnamesearch")
    workflowResource.makePublic(wid, ownerSession)
    relabel(wid, "bbrenamedwhilefollowing", "a workflow")

    searchWids(
      anonymous,
      SearchQueryParams(keywords = keywords("bbrenamedwhilefollowing"))
    ) should contain(wid)
  }

  it should "still match the author's own workflow on a name only they can see" in {
    val wid = createWorkflow("ccownnamesearch")
    publishPinned(wid)
    relabel(wid, "ccprivaterenamesearch", "a workflow")

    searchWids(
      ownerSession,
      SearchQueryParams(keywords = keywords("ccprivaterenamesearch"))
    ) should contain(wid)
  }

  it should "show a public viewer the published name and description in a listing" in {
    // The listing is where people find and click a workflow, so a title that keeps following the
    // author defeats the freeze exactly as an unfrozen graph would: a report about a title could be
    // answered by quietly editing the title.
    val wid = createWorkflow("listing_name_before")
    publishPinned(wid)
    relabel(wid, "listing_name_after", "described after publishing")

    def listedAs(session: SessionUser): (String, String) = {
      val entry = listingOf(session, wid).workflow
      (entry.getName, entry.getDescription)
    }

    // The author is not a public viewer of their own workflow.
    listedAs(ownerSession) shouldBe ("listing_name_after", "described after publishing")
    // A stranger reaches it only because it is public.
    listedAs(strangerSession) shouldBe ("listing_name_before", "a workflow")
    // ...and the listing now agrees with what opening it shows.
    workflowResource.retrievePublicWorkflow(wid).name shouldBe "listing_name_before"
  }

  it should "show a public viewer the author's live name while nothing is pinned" in {
    val wid = createWorkflow("listing_unpinned_before")
    workflowResource.makePublic(wid, ownerSession)
    relabel(wid, "listing_unpinned_after", "a workflow")

    listingOf(strangerSession, wid).workflow.getName shouldBe "listing_unpinned_after"
  }

  it should "show a collaborator the author's live name in a listing" in {
    val wid = createWorkflow("listing_for_collaborator")
    publishPinned(wid)
    grantAccess(wid, PrivilegeEnum.READ)

    try {
      relabel(wid, "renamed_after_sharing", "a workflow")
      listingOf(strangerSession, wid).workflow.getName shouldBe "renamed_after_sharing"
    } finally revokeAccess(wid)
  }

  it should "leave a private workflow's own listing untouched" in {
    val wid = createWorkflow("listing_private")
    relabel(wid, "private_renamed", "a workflow")

    listingOf(ownerSession, wid).workflow.getName shouldBe "private_renamed"
  }

  it should "tell listings when the copy on show is behind the author's working copy" in {
    // What sends the author to the published preview instead of their editor when they click their
    // own workflow in the hub: the entry is advertising the pinned version, not what they are editing.
    val wid = createWorkflow("listing_reports_drift")
    publishPinned(wid)

    def drifted(): Boolean = listingOf(ownerSession, wid).hasUnpublishedChanges

    drifted() shouldBe false
    edit(wid, editedContent)
    drifted() shouldBe true
    workflowResource.pinLatest(wid, ownerSession)
    drifted() shouldBe false
  }

  it should "report the same drift to a listing as to the share dialog" in {
    // Three places answer "is the public behind?": the share dialog in Scala, the search projection
    // and the hub listing in SQL. A rename or a change of view exercises the fields most easily left
    // out of one of them, and a card that disagrees with the dialog is a card the author distrusts.
    val wid = createWorkflow("listing_matches_dialog")
    publishPinned(wid)

    relabel(wid, "renamed_after_pinning", "a workflow")
    listingOf(ownerSession, wid).hasUnpublishedChanges shouldBe statusOf(wid).hasUnpublishedChanges
    listingOf(ownerSession, wid).hasUnpublishedChanges shouldBe true

    workflowResource.pinLatest(wid, ownerSession)
    getDSLContext
      .update(WORKFLOW)
      .set(WORKFLOW.DEFAULT_VIEW, DefaultViewEnum.FORM)
      .where(WORKFLOW.WID.eq(wid))
      .execute()

    listingOf(ownerSession, wid).hasUnpublishedChanges shouldBe statusOf(wid).hasUnpublishedChanges
    listingOf(ownerSession, wid).hasUnpublishedChanges shouldBe true
  }

  it should "never report drift for a workflow with nothing frozen" in {
    // Drift is "what the public sees is not what you have", so it can only be true of a workflow that
    // has a frozen copy at all. Private and public-but-following both have none.
    def driftOf(wid: Integer): Boolean = listingOf(ownerSession, wid).hasUnpublishedChanges

    val priv = createWorkflow("listing_ignores_private")
    edit(priv, editedContent)
    driftOf(priv) shouldBe false

    val following = createWorkflow("listing_ignores_unpinned")
    workflowResource.makePublic(following, ownerSession)
    edit(following, editedContent)
    driftOf(following) shouldBe false
  }
}
