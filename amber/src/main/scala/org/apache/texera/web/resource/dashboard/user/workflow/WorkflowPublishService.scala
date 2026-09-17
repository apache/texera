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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{WORKFLOW, WORKFLOW_VERSION}
import org.apache.texera.dao.jooq.generated.enums.DefaultViewEnum
import org.apache.texera.dao.jooq.generated.tables.daos.WorkflowDao
import org.apache.texera.dao.jooq.generated.tables.pojos.Workflow
import org.jooq.{Condition, DSLContext}

import javax.ws.rs.NotFoundException
import scala.jdk.CollectionConverters.CollectionHasAsScala

import java.sql.Timestamp
import scala.util.Try

/**
  * Version pinning for public workflows.
  *
  * A public workflow follows the author's latest, as publishing has always done, until the author
  * pins the version they have now: the public then keeps seeing that frozen copy while the author's
  * later edits stay in the workflow's own columns until they pin again.
  *
  * `is_public` stays the on/off switch; `published_content` is the pin, NULL while following. A pin
  * freezes everything on public show -- the graph, the title, the description and the view it opens
  * in -- because a copy that froze only its graph would still advertise a title nobody published.
  *
  * Not to be confused with sharing: a user granted access always tracks the author's latest, pin or
  * no pin. Only viewers who arrive because the workflow is public are held at the frozen copy.
  */
object WorkflowPublishService extends LazyLogging {

  private def context: DSLContext = SqlServer.getInstance().createDSLContext()

  /**
    * What the share dialog asks about: whether the workflow is public, whether a version is pinned,
    * and whether that pin is holding edits back -- the last is true when pinning again would publish
    * something, and always false while following.
    *
    * @param pinnedVersionTime the date naming the pinned version in both the dialog and the
    *                           revision panel.
    */
  case class PublishStatus(
      isPublished: Boolean,
      isPinned: Boolean,
      pinnedVersionTime: Option[Timestamp],
      hasUnpublishedChanges: Boolean
  )

  /**
    * Whether two workflow contents describe the same graph. Compared as parsed trees, because the
    * two blobs travel by different routes and the same graph can come back with its whitespace or
    * key order rearranged -- reporting that as an edit the public cannot see would be an alarm the
    * author cannot clear.
    */
  private def sameContent(a: String, b: String): Boolean =
    a == b || Try(objectMapper.readTree(a) == objectMapper.readTree(b)).getOrElse(false)

  /** The workflow, or a 404. */
  private def requireWorkflow(wid: Integer): Workflow =
    Option(new WorkflowDao(context.configuration).fetchOneByWid(wid))
      .getOrElse(throw new NotFoundException(s"Workflow $wid not found"))

  /**
    * Turns publishing on, and touches nothing else. A workflow coming back from private is
    * following the author's latest, because unpublishing always drops the pin: coming back should
    * not silently put old public content back on show. Called on a workflow that is already public
    * it changes nothing, pin included.
    */
  def publish(wid: Integer): PublishStatus = {
    val updated = context
      .update(WORKFLOW)
      .set(WORKFLOW.IS_PUBLIC, java.lang.Boolean.TRUE)
      .where(WORKFLOW.WID.eq(wid))
      .execute()
    if (updated == 0) {
      throw new NotFoundException(s"Workflow $wid not found")
    }
    // Deliberately not "following latest": this turns publishing on and touches nothing else, so a
    // workflow that somehow arrives here already pinned stays pinned.
    logger.info(s"Workflow $wid published")
    statusOf(wid)
  }

  /**
    * Freezes the author's current copy as the public one, and turns publishing on. The title, the
    * description and the default view freeze with the graph: they are as public as it is, and the
    * database refuses a pinned copy that carries only part of itself. The view matters because a
    * form's definition rides inside the content -- serving the live preference over a frozen graph
    * would open a form on a copy that has none.
    *
    * Each column is copied from its own row rather than from a workflow read a moment earlier, so
    * there is no window in which the author's next save lands and the pin freezes the version
    * before it -- which would leave them looking at "you have unpublished changes" the instant
    * after they pinned.
    *
    * `versionId` names the row in the revision history that replays to this copy, so the history can
    * mark which version is the published one.
    */
  private def writePin(ctx: DSLContext, wid: Integer, versionId: Integer): Unit =
    ctx
      .update(WORKFLOW)
      .set(WORKFLOW.IS_PUBLIC, java.lang.Boolean.TRUE)
      .set(WORKFLOW.PUBLISHED_VERSION_ID, versionId)
      .set(WORKFLOW.PUBLISHED_CONTENT, WORKFLOW.CONTENT)
      .set(WORKFLOW.PUBLISHED_NAME, WORKFLOW.NAME)
      .set(WORKFLOW.PUBLISHED_DESCRIPTION, WORKFLOW.DESCRIPTION)
      .set(WORKFLOW.PUBLISHED_DEFAULT_VIEW, WORKFLOW.DEFAULT_VIEW)
      .where(WORKFLOW.WID.eq(wid))
      .execute()

  /**
    * Clears the pinned copy in one statement, optionally unpublishing too: the constraint accepts a
    * row only with every frozen column set on a public workflow, or with every one of them NULL, so
    * clearing them one at a time -- or clearing them after `is_public` -- would be rejected.
    *
    * @return how many rows it matched, so a missing workflow is distinguishable from a done one.
    */
  private def clearPin(wid: Integer, alsoUnpublish: Boolean = false): Int = {
    val cleared = context
      .update(WORKFLOW)
      .set(WORKFLOW.PUBLISHED_VERSION_ID, null.asInstanceOf[Integer])
      .set(WORKFLOW.PUBLISHED_CONTENT, null.asInstanceOf[String])
      .set(WORKFLOW.PUBLISHED_NAME, null.asInstanceOf[String])
      .set(WORKFLOW.PUBLISHED_DESCRIPTION, null.asInstanceOf[String])
      .set(WORKFLOW.PUBLISHED_DEFAULT_VIEW, null.asInstanceOf[DefaultViewEnum])
    val statement =
      if (alsoUnpublish) cleared.set(WORKFLOW.IS_PUBLIC, java.lang.Boolean.FALSE) else cleared
    statement.where(WORKFLOW.WID.eq(wid)).execute()
  }

  /** Pins the current content as the public copy. Moving a pin forward is the same operation. */
  def pinLatest(wid: Integer): PublishStatus = {
    context.transaction { txConfig =>
      val ctx = org.jooq.impl.DSL.using(txConfig)
      // Locked, not merely read: two pins racing would both read before either wrote, and both
      // insert an anchor.
      val workflow = ctx
        .selectFrom(WORKFLOW)
        .where(WORKFLOW.WID.eq(wid))
        .forUpdate()
        .fetchOneInto(classOf[Workflow])
      if (workflow == null) {
        throw new NotFoundException(s"Workflow $wid not found")
      }

      // An anchor in the revision history for the copy being pinned: its delta is the identity patch,
      // so replaying this row returns what was published however many edits pile up later. An
      // existing version row cannot stand in, since one replays to the content as it was *before*
      // the change it records. Pinning again unchanged reuses the anchor rather than adding a twin.
      val anchorVid = Option(workflow.getPublishedVersionId)
        .filter(_ =>
          Option(workflow.getPublishedContent).exists(sameContent(_, workflow.getContent))
        )
        .getOrElse(WorkflowVersionResource.insertNewVersion(wid, ctx = ctx).getVid)

      writePin(ctx, wid, anchorVid)
    }
    logger.info(s"Workflow $wid pinned to its latest content")
    statusOf(wid)
  }

  /**
    * Drops the pin, so the public follows the author's latest again. The workflow stays public.
    */
  def unpin(wid: Integer): PublishStatus = {
    if (clearPin(wid) == 0) {
      throw new NotFoundException(s"Workflow $wid not found")
    }
    logger.info(s"Workflow $wid unpinned, following latest")
    statusOf(wid)
  }

  /**
    * Turns publishing off and drops the pin. Publishing again starts in the following state; the
    * previous frozen copy is deliberately not remembered, so an unpublish/re-publish cycle cannot
    * silently restore old public content.
    */
  def unpublish(wid: Integer): Unit = {
    if (clearPin(wid, alsoUnpublish = true) == 0) {
      throw new NotFoundException(s"Workflow $wid not found")
    }
    logger.info(s"Workflow $wid unpublished")
  }

  /** Read from the version row, so the dialog and the revision panel print one date rather than two. */
  private def pinnedVersionTimeOf(versionId: Integer): Option[Timestamp] =
    Option(
      context
        .select(WORKFLOW_VERSION.CREATION_TIME)
        .from(WORKFLOW_VERSION)
        .where(WORKFLOW_VERSION.VID.eq(versionId))
        .fetchOneInto(classOf[Timestamp])
    )

  /** The date a public viewer should see: that of the version on show, not of an edit they cannot. */
  def publicModifiedTime(workflow: Workflow): Timestamp =
    Option(workflow.getPublishedVersionId)
      .flatMap(pinnedVersionTimeOf)
      .getOrElse(workflow.getLastModifiedTime)

  /** Whether a version is pinned, and whether it is holding edits back. */
  def statusOf(wid: Integer): PublishStatus = {
    val workflow = requireWorkflow(wid)
    val pinned = workflow.getPublishedContent != null
    PublishStatus(
      isPublished = workflow.getIsPublic,
      isPinned = pinned,
      pinnedVersionTime = Option(workflow.getPublishedVersionId).flatMap(pinnedVersionTimeOf),
      // Literally "what the public sees is not what you have": whatever [[publicCopyOf]] freezes is
      // what this compares, on values rather than version ids, so an edit and its undo cancel out.
      hasUnpublishedChanges = differs(publicCopyOf(workflow), workingCopyOf(workflow))
    )
  }

  /**
    * Every field of the copy, so that a rename the public cannot see is held back exactly as an edit
    * to the graph is. Content is compared as a tree: a restore can rearrange whitespace, and calling
    * that drift alarms nobody.
    */
  private def differs(public: PublicCopy, working: PublicCopy): Boolean =
    public.name != working.name ||
      public.description != working.description ||
      public.defaultView != working.defaultView ||
      !sameContent(public.content, working.content)

  /**
    * Everything about a workflow that is on public show, carried together so that a caller cannot
    * serve the frozen graph under the author's live title, or open the author's chosen view on a
    * copy that does not contain it.
    */
  case class PublicCopy(
      name: String,
      description: String,
      content: String,
      defaultView: DefaultViewEnum
  )

  /** What every public surface must serve, as a group so no field is the one that gets forgotten. */
  def publicCopyOf(workflow: Workflow): PublicCopy =
    if (workflow.getPublishedContent == null) workingCopyOf(workflow)
    else
      PublicCopy(
        workflow.getPublishedName,
        workflow.getPublishedDescription,
        workflow.getPublishedContent,
        workflow.getPublishedDefaultView
      )

  /** The author's own copy, in the same shape. */
  private def workingCopyOf(workflow: Workflow): PublicCopy =
    PublicCopy(
      workflow.getName,
      workflow.getDescription,
      workflow.getContent,
      workflow.getDefaultView
    )

  /**
    * [[differs]] as a condition, for the listings that ask about many workflows at once: the same
    * fields, so a card and the share dialog can never disagree about whether edits are held back.
    * Only meaningful on a row that is pinned -- while following, every frozen column is NULL and
    * `isDistinctFrom` would read that as drift.
    */
  val pinDiffersFromWorkingCopy: Condition =
    WORKFLOW.PUBLISHED_CONTENT
      .isDistinctFrom(WORKFLOW.CONTENT)
      .or(WORKFLOW.PUBLISHED_NAME.isDistinctFrom(WORKFLOW.NAME))
      .or(WORKFLOW.PUBLISHED_DESCRIPTION.isDistinctFrom(WORKFLOW.DESCRIPTION))
      .or(WORKFLOW.PUBLISHED_DEFAULT_VIEW.isDistinctFrom(WORKFLOW.DEFAULT_VIEW))

  /**
    * What a listing needs about one pinned workflow: the frozen name and description it must show
    * instead of the author's live ones, and whether those live ones have moved on.
    */
  case class PinnedListing(name: String, description: String, hasUnpublishedChanges: Boolean)

  /**
    * The pinned listings among `wids`, keyed by wid. A workflow that follows the author's latest is
    * simply absent, which leaves its live values in place and its drift flag false.
    *
    * Drift is decided in SQL here rather than by [[differs]], because a listing asks about many
    * workflows at once and none of their contents are worth shipping back to compare in memory.
    */
  def pinnedListingsOf(wids: Seq[Integer]): Map[Integer, PinnedListing] =
    if (wids.isEmpty) Map()
    else {
      val drifted = pinDiffersFromWorkingCopy
      context
        .select(WORKFLOW.WID, WORKFLOW.PUBLISHED_NAME, WORKFLOW.PUBLISHED_DESCRIPTION, drifted)
        .from(WORKFLOW)
        .where(WORKFLOW.WID.in(wids: _*).and(WORKFLOW.PUBLISHED_CONTENT.isNotNull))
        .fetch()
        .asScala
        .map(row =>
          row.get(WORKFLOW.WID) -> PinnedListing(
            row.get(WORKFLOW.PUBLISHED_NAME),
            row.get(WORKFLOW.PUBLISHED_DESCRIPTION),
            row.get(drifted)
          )
        )
        .toMap
    }

  /** As [[publicCopyOf]], for callers holding only a wid. 404s unless the workflow is public. */
  def publicCopyOf(wid: Integer): PublicCopy = {
    val workflow = requireWorkflow(wid)
    if (!workflow.getIsPublic) {
      throw new NotFoundException(s"Workflow $wid is not public")
    }
    publicCopyOf(workflow)
  }
}
