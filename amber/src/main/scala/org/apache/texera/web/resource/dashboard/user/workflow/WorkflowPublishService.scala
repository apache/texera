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
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW
import org.apache.texera.dao.jooq.generated.enums.DefaultViewEnum
import org.apache.texera.dao.jooq.generated.tables.daos.WorkflowDao
import org.apache.texera.dao.jooq.generated.tables.pojos.Workflow
import org.jooq.DSLContext

import javax.ws.rs.NotFoundException
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
    */
  case class PublishStatus(
      isPublished: Boolean,
      isPinned: Boolean,
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
    logger.info(s"Workflow $wid published, following latest")
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
    * @return how many rows it matched, so a missing workflow is distinguishable from a done one.
    */
  private def writePin(wid: Integer): Int =
    context
      .update(WORKFLOW)
      .set(WORKFLOW.IS_PUBLIC, java.lang.Boolean.TRUE)
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
    * `published_version_id` is named by that constraint as well but is not touched here, for the
    * same reason [[writePin]] does not set it: nothing writes it yet, so it is NULL on every row.
    *
    * @return how many rows it matched, so a missing workflow is distinguishable from a done one.
    */
  private def clearPin(wid: Integer, alsoUnpublish: Boolean = false): Int = {
    val cleared = context
      .update(WORKFLOW)
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
    if (writePin(wid) == 0) {
      throw new NotFoundException(s"Workflow $wid not found")
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

  /** Whether a version is pinned, and whether it is holding edits back. */
  def statusOf(wid: Integer): PublishStatus = {
    val workflow = requireWorkflow(wid)
    val pinned = workflow.getPublishedContent != null
    PublishStatus(
      isPublished = workflow.getIsPublic,
      isPinned = pinned,
      // Literally "what the public sees is not what you have". Every field the pin freezes counts:
      // a rename the public cannot see is held back exactly as an edit to the graph is. Compared on
      // values rather than on a version id, so that an edit and its undo report nothing held back.
      hasUnpublishedChanges = pinned && (
        !sameContent(workflow.getPublishedContent, workflow.getContent) ||
          workflow.getPublishedName != workflow.getName ||
          workflow.getPublishedDescription != workflow.getDescription ||
          workflow.getPublishedDefaultView != workflow.getDefaultView
      )
    )
  }
}
