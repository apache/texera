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

package org.apache.texera.web.resource.dashboard

import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.tables.pojos.Workflow
import org.apache.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import org.apache.texera.web.resource.dashboard.FulltextSearchQueryUtils._
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowPublishService
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import org.jooq.impl.DSL
import org.jooq.{Condition, Field, GroupField, Record, TableLike}

import scala.jdk.CollectionConverters.CollectionHasAsScala
import org.apache.texera.dao.jooq.generated.enums.{DefaultViewEnum, PrivilegeEnum}

object WorkflowSearchQueryBuilder extends SearchQueryBuilder {

  override val mappedResourceSchema: UnifiedResourceSchema = {
    UnifiedResourceSchema(
      resourceType = DSL.inline(SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE),
      name = WORKFLOW.NAME,
      description = WORKFLOW.DESCRIPTION,
      creationTime = WORKFLOW.CREATION_TIME,
      wid = WORKFLOW.WID,
      lastModifiedTime = WORKFLOW.LAST_MODIFIED_TIME,
      executionTime = DSL.field(
        DSL
          .select(DSL.max(WORKFLOW_EXECUTIONS.STARTING_TIME))
          .from(WORKFLOW_EXECUTIONS)
          .join(WORKFLOW_VERSION)
          .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
          .where(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
      ),
      workflowUserAccess = WORKFLOW_USER_ACCESS.PRIVILEGE,
      uid = WORKFLOW_OF_USER.UID,
      ownerId = WORKFLOW_OF_USER.UID,
      userName = USER.NAME,
      workflowCoverImage = DSL.max(WORKFLOW_COVER_IMAGE.IMAGE).as("workflow_cover_image"),
      workflowDefaultView = WORKFLOW.DEFAULT_VIEW.as("workflow_default_view"),
      // The isNotNull guard because isDistinctFrom would read a NULL pin as different content, and
      // every unpinned public workflow would report drift. Aggregated to stay out of the GROUP BY,
      // which would otherwise group the search by two TEXT columns.
      workflowHasUnpublishedChanges = DSL
        .boolOr(
          WORKFLOW.PUBLISHED_CONTENT.isNotNull
            .and(WorkflowPublishService.pinDiffersFromWorkingCopy)
        )
        .as("workflow_has_unpublished_changes"),
      // What a viewer without granted access is shown instead of the author's live metadata. NULL
      // while following, which the reader treats as "show the live values".
      workflowPublishedName = DSL.max(WORKFLOW.PUBLISHED_NAME).as("workflow_published_name"),
      workflowPublishedDescription =
        DSL.max(WORKFLOW.PUBLISHED_DESCRIPTION).as("workflow_published_description"),
      // The access join is already restricted to the caller, so this needs no user id of its own.
      viewerHasGrantedAccess = DSL
        .boolOr(WORKFLOW_USER_ACCESS.UID.isNotNull)
        .as("viewer_has_granted_access")
    )
  }

  override protected def constructFromClause(
      uid: Integer,
      params: DashboardResource.SearchQueryParams,
      includePublic: Boolean = false
  ): TableLike[_] = {
    val baseQuery = WORKFLOW
      .leftJoin(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
      .and(if (uid == null) DSL.falseCondition() else WORKFLOW_USER_ACCESS.UID.eq(uid))
      .leftJoin(WORKFLOW_OF_USER)
      .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
      .leftJoin(USER)
      .on(USER.UID.eq(WORKFLOW_OF_USER.UID))
      .leftJoin(WORKFLOW_COVER_IMAGE)
      .on(WORKFLOW_COVER_IMAGE.WID.eq(WORKFLOW.WID))

    var condition: Condition = DSL.trueCondition()
    if (uid == null) {
      condition = WORKFLOW.IS_PUBLIC.eq(true)
    } else {
      val privateAccessCondition = WORKFLOW_USER_ACCESS.UID.eq(uid)
      if (includePublic) {
        condition = privateAccessCondition.or(WORKFLOW.IS_PUBLIC.eq(true))
      } else {
        condition = privateAccessCondition
      }
    }

    baseQuery.where(condition)
  }

  /** Rows the user was granted access to, as opposed to rows they see only because it is public. */
  private def grantedAccessCondition(uid: Integer): Condition =
    if (uid == null) DSL.falseCondition()
    else WORKFLOW_USER_ACCESS.UID.eq(uid)

  /**
    * The three searchable columns carrying one copy. They travel together because a pin freezes them
    * together: a filter over content alone would match a pinned workflow on a title no public viewer
    * has seen.
    */
  private case class WorkflowCopy(
      name: Field[String],
      description: Field[String],
      content: Field[String]
  )

  private val workingCopy =
    WorkflowCopy(WORKFLOW.NAME, WORKFLOW.DESCRIPTION, WORKFLOW.CONTENT)
  private val pinnedCopy =
    WorkflowCopy(
      WORKFLOW.PUBLISHED_NAME,
      WORKFLOW.PUBLISHED_DESCRIPTION,
      WORKFLOW.PUBLISHED_CONTENT
    )

  /**
    * Applies a filter to whichever copy the user is allowed to see: one search can return both their
    * own workflows and public ones, and a pinned public one must not turn up on keywords that exist
    * only behind the pin. A disjunction of guarded filters over bare columns rather than a CASE, so
    * each side stays eligible for its own PGroonga index.
    */
  private def onVisibleCopy(
      uid: Integer,
      includePublic: Boolean
  )(build: WorkflowCopy => Condition): Condition = {
    val onWorkingCopy = build(workingCopy).and(grantedAccessCondition(uid))
    val onPublicCopy = WORKFLOW.IS_PUBLIC
      .eq(true)
      .and(
        // Following leaves the pinned columns NULL, and the public copy is then the working one.
        build(pinnedCopy)
          .or(WORKFLOW.PUBLISHED_CONTENT.isNull.and(build(workingCopy)))
      )
    if (uid == null) onPublicCopy
    else if (includePublic) onWorkingCopy.or(onPublicCopy)
    else onWorkingCopy
  }

  override protected def constructWhereClause(
      uid: Integer,
      params: DashboardResource.SearchQueryParams,
      includePublic: Boolean
  ): Condition = {
    val splitKeywords = params.keywords.asScala
      .flatMap(_.split("[+\\-()<>~*@\"]"))
      .filter(_.nonEmpty)
      .toSeq
    getDateFilter(
      params.creationStartDate,
      params.creationEndDate,
      WORKFLOW.CREATION_TIME
    )
      // Apply lastModified_time date filter
      .and(
        getDateFilter(
          params.modifiedStartDate,
          params.modifiedEndDate,
          WORKFLOW.LAST_MODIFIED_TIME
        )
      )
      // Apply workflowID filter
      .and(getContainsFilter(params.workflowIDs, WORKFLOW.WID))
      // Apply owner filter
      .and(getContainsFilter(params.owners, USER.EMAIL))
      // Apply operators filter
      .and(
        if (params.operators.isEmpty) DSL.noCondition()
        else
          onVisibleCopy(uid, includePublic)(copy =>
            getOperatorsFilter(params.operators, copy.content)
          )
      )
      // Apply fulltext search filter
      .and(
        if (splitKeywords.isEmpty) DSL.noCondition()
        else
          onVisibleCopy(uid, includePublic)(copy =>
            getFullTextSearchFilter(
              splitKeywords,
              List(copy.name, copy.description, copy.content)
            )
          )
      )
  }

  override protected def getGroupByFields: Seq[GroupField] = {
    Seq(
      WORKFLOW.NAME,
      WORKFLOW.DESCRIPTION,
      WORKFLOW.CREATION_TIME,
      WORKFLOW.WID,
      WORKFLOW.LAST_MODIFIED_TIME,
      WORKFLOW_USER_ACCESS.PRIVILEGE,
      WORKFLOW_OF_USER.UID,
      USER.NAME
    )
  }

  override def toEntryImpl(
      uid: Integer,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val workflow = record.into(WORKFLOW).into(classOf[Workflow])
    // The select lists specific columns, so the POJO built from the record does not carry this one.
    // Without it the listing forgets the default-view preference on every refresh.
    workflow.setDefaultView(record.get("workflow_default_view", classOf[DefaultViewEnum]))

    // A viewer here only because the workflow is public sees the pinned name and description, the
    // same copy the detail page serves -- otherwise a listing would advertise a title that opening it
    // does not show. Both are NULL while following, which leaves the live values in place.
    // Unknown counts as not granted: the reverse is the leak.
    val granted = Option(record.get("viewer_has_granted_access", classOf[java.lang.Boolean]))
      .exists(_.booleanValue())
    if (!granted) {
      Option(record.get("workflow_published_name", classOf[String])).foreach(workflow.setName)
      Option(record.get("workflow_published_description", classOf[String]))
        .foreach(workflow.setDescription)
    }

    val dw = DashboardWorkflow(
      record.into(WORKFLOW_OF_USER).getUid == uid,
      Option(record.get(WORKFLOW_USER_ACCESS.PRIVILEGE, classOf[PrivilegeEnum]))
        .map(_.toString)
        .getOrElse(PrivilegeEnum.NONE.toString),
      record.into(USER).getName,
      workflow,
      record.into(USER).getUid,
      Option(record.get("workflow_cover_image", classOf[String])),
      // Null for the resource types that do not define the column at all.
      Option(record.get("workflow_has_unpublished_changes", classOf[java.lang.Boolean]))
        .exists(_.booleanValue())
    )
    DashboardClickableFileEntry(SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE, workflow = Some(dw))
  }
}
