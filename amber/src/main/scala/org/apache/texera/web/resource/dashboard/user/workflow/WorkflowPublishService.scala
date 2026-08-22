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
import org.apache.texera.dao.jooq.generated.tables.daos.WorkflowDao
import org.apache.texera.dao.jooq.generated.tables.pojos.Workflow
import org.jooq.DSLContext

import scala.util.Try
import javax.ws.rs.NotFoundException

/**
  * Version pinning for public workflows.
  *
  * A public workflow follows the author's latest content, as publishing has always done, until the
  * author pins the version they have now: the public then keeps seeing that frozen copy while the
  * author's later edits stay in `workflow.content` until they pin again.
  *
  * `is_public` stays the on/off switch; `published_content` is the pin, NULL while following.
  *
  * Not to be confused with sharing: a user granted access always tracks the author's latest, pin or
  * no pin. Only viewers who arrive because the workflow is public are held at the frozen copy --
  * which the public read paths take up next.
  */
object WorkflowPublishService extends LazyLogging {

  private def context: DSLContext = SqlServer.getInstance().createDSLContext()

  /**
    * @param hasUnpublishedChanges true when a pin is holding edits back, i.e. pinning again would
    *                              publish them. Always false while following.
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
    * Makes the workflow public, following the author's latest. A pin from a previous publication is
    * not restored: coming back should not silently put old public content on show again.
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

  /** Writes the pin. Name and description freeze with the graph, or a pin would leak the working title. */
  private def writePin(workflow: Workflow, content: String): Unit =
    context
      .update(WORKFLOW)
      .set(WORKFLOW.IS_PUBLIC, java.lang.Boolean.TRUE)
      .set(WORKFLOW.PUBLISHED_CONTENT, content)
      .set(WORKFLOW.PUBLISHED_NAME, workflow.getName)
      .set(WORKFLOW.PUBLISHED_DESCRIPTION, workflow.getDescription)
      .where(WORKFLOW.WID.eq(workflow.getWid))
      .execute()

  /**
    * Clears the pinned copy in one statement, optionally unpublishing too: the CHECK constraint
    * holds only while the copy and `is_public` move together.
    *
    * @return how many rows it matched, so a missing workflow is distinguishable from a done one.
    */
  private def clearPin(wid: Integer, alsoUnpublish: Boolean = false): Int = {
    val cleared = context
      .update(WORKFLOW)
      .set(WORKFLOW.PUBLISHED_CONTENT, null.asInstanceOf[String])
      .set(WORKFLOW.PUBLISHED_NAME, null.asInstanceOf[String])
      .set(WORKFLOW.PUBLISHED_DESCRIPTION, null.asInstanceOf[String])
    val statement =
      if (alsoUnpublish) cleared.set(WORKFLOW.IS_PUBLIC, java.lang.Boolean.FALSE) else cleared
    statement.where(WORKFLOW.WID.eq(wid)).execute()
  }

  /** Pins the current content as the public copy. Moving a pin forward is the same operation. */
  def pinLatest(wid: Integer): PublishStatus = {
    val workflow = requireWorkflow(wid)
    writePin(workflow, workflow.getContent)
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
    clearPin(wid, alsoUnpublish = true)
    logger.info(s"Workflow $wid unpublished")
  }

  /** Whether a version is pinned, and whether it is holding edits back. */
  def statusOf(wid: Integer): PublishStatus = statusOf(requireWorkflow(wid))

  def statusOf(workflow: Workflow): PublishStatus =
    PublishStatus(
      isPublished = workflow.getIsPublic,
      isPinned = workflow.getPublishedContent != null,
      // Literally "what the public sees is not what you have": whatever [[publicCopyOf]] freezes is
      // what this compares, on values rather than version ids, so an edit and its undo cancel out.
      hasUnpublishedChanges = differs(publicCopyOf(workflow), workingCopyOf(workflow))
    )

  /** Compared as a tree: a restore can rearrange whitespace, and calling that drift alarms nobody. */
  private def differs(public: PublicCopy, working: PublicCopy): Boolean =
    public.name != working.name ||
      public.description != working.description ||
      !sameContent(public.content, working.content)

  /** Everything about a workflow that is on public show. */
  case class PublicCopy(name: String, description: String, content: String)

  /** What every public surface must serve, as a group so no field is the one that gets forgotten. */
  def publicCopyOf(workflow: Workflow): PublicCopy =
    if (workflow.getPublishedContent == null) workingCopyOf(workflow)
    else
      PublicCopy(
        workflow.getPublishedName,
        workflow.getPublishedDescription,
        workflow.getPublishedContent
      )

  /** The author's own copy, in the same shape. */
  private def workingCopyOf(workflow: Workflow): PublicCopy =
    PublicCopy(workflow.getName, workflow.getDescription, workflow.getContent)

  /** As [[publicCopyOf]], for callers holding only a wid. 404s unless the workflow is public. */
  def publicCopyOf(wid: Integer): PublicCopy = {
    val workflow = requireWorkflow(wid)
    if (!workflow.getIsPublic) {
      throw new NotFoundException(s"Workflow $wid is not public")
    }
    publicCopyOf(workflow)
  }
}
