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
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import org.jooq.impl.{DSL => JDSL}
import org.jooq.{Record, SQLDialect}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Covers `WorkflowSearchQueryBuilder.toEntryImpl`, the pure record-to-DTO
  * mapping, plus the one literal in `mappedResourceSchema` that the dashboard
  * dispatch depends on. Both members widen the trait's `protected` to public,
  * which is what makes them reachable from a spec at all.
  *
  * Breakage this catches:
  *   - the `projects` aggregate lookup breaking (a Field looked up by structural
  *     equality): `record.get(pidField)` would start returning null and every
  *     workflow would silently report zero projects instead of failing;
  *   - the comma-split branch losing/reordering ids, or the NULL branch no
  *     longer yielding an empty list;
  *   - a NULL `workflow_user_access.privilege` (a left join miss for public
  *     workflows) no longer degrading to NONE — an NPE or a wrong grant;
  *   - the ownership flag comparing the wrong uid, or owner id/name being read off
  *     the wrong column: two record columns are called `name` (WORKFLOW.NAME and
  *     USER.NAME), and `ownerId` resolves only because the unqualified name `uid`
  *     matches `workflow_of_user.uid` — the projection never selects USER.UID;
  *   - the inline `'workflow'` tag changing, which turns every "all resources"
  *     search into a MatchError in DashboardResource.searchAllResources.
  *
  * `constructFromClause`, `constructWhereClause` and `getGroupByFields` stay
  * `override protected`, and Scala `protected` grants no same-package access, so
  * they are not callable from here. (They are otherwise pure jOOQ DSL builders —
  * the access modifier is the only thing keeping them out of scope.)
  *
  * Nothing here reads or writes the shared global
  * `FulltextSearchQueryUtils.usePgroonga`, so this spec neither depends on it
  * nor mutates it.
  */
class WorkflowSearchQueryBuilderSpec extends AnyFlatSpec with Matchers {

  // jOOQ render context (Postgres dialect to match production renderers).
  private val ctx = JDSL.using(SQLDialect.POSTGRES)

  // toEntryImpl re-creates this aggregate locally and then looks it up with
  // `record.get(pidField)`; the lookup only resolves because jOOQ compares
  // QueryParts structurally. The first test pins that assumption.
  private val pidField = JDSL.groupConcatDistinct(WORKFLOW_OF_PROJECT.PID)
  private val coverField = JDSL.max(WORKFLOW_COVER_IMAGE.IMAGE).as("workflow_cover_image")
  private val driftField =
    JDSL.field(JDSL.name("workflow_has_unpublished_changes"), classOf[java.lang.Boolean])
  private val publishedNameField = JDSL.field(JDSL.name("workflow_published_name"), classOf[String])
  private val publishedDescriptionField =
    JDSL.field(JDSL.name("workflow_published_description"), classOf[String])
  private val grantedField =
    JDSL.field(JDSL.name("viewer_has_granted_access"), classOf[java.lang.Boolean])

  private val ownerUid: Integer = Integer.valueOf(42)
  private val viewerUid: Integer = Integer.valueOf(43)
  private val wid: Integer = Integer.valueOf(7)

  /**
    * Builds the subset of the *translated* record (see
    * `UnifiedResourceSchema.translateRecord`, whose output is keyed by the
    * original fields) that toEntryImpl actually reads. `newRecord` is pure
    * in-memory jOOQ — no connection is involved.
    *
    * Every value is distinct so that reading the wrong column cannot pass.
    */
  private def translatedRecord(
      uidValue: Integer = ownerUid,
      privilege: PrivilegeEnum = PrivilegeEnum.WRITE,
      projects: String = "3,1,2",
      cover: String = "cover-b64",
      hasUnpublishedChanges: java.lang.Boolean = null,
      // The rows these tests describe belong to a viewer who was granted access, which is what
      // leaves the author's own name and description in place.
      grantedAccess: java.lang.Boolean = java.lang.Boolean.TRUE,
      publishedName: String = null,
      publishedDescription: String = null
  ): Record = {
    val record = ctx.newRecord(
      WORKFLOW.WID,
      WORKFLOW.NAME,
      WORKFLOW.DESCRIPTION,
      WORKFLOW_OF_USER.UID,
      WORKFLOW_USER_ACCESS.PRIVILEGE,
      USER.NAME,
      pidField,
      coverField,
      driftField,
      publishedNameField,
      publishedDescriptionField,
      grantedField
    )
    record.set(WORKFLOW.WID, wid)
    record.set(WORKFLOW.NAME, "wf-name")
    record.set(WORKFLOW.DESCRIPTION, "wf-description")
    record.set(WORKFLOW_OF_USER.UID, uidValue)
    record.set(WORKFLOW_USER_ACCESS.PRIVILEGE, privilege)
    record.set(USER.NAME, "owner-name")
    record.set(pidField, projects)
    record.set(coverField, cover)
    record.set(driftField, hasUnpublishedChanges)
    record.set(publishedNameField, publishedName)
    record.set(publishedDescriptionField, publishedDescription)
    record.set(grantedField, grantedAccess)
    record
  }

  private def workflowOf(record: Record, uid: Integer): DashboardWorkflow =
    WorkflowSearchQueryBuilder.toEntryImpl(uid, record).workflow.get

  // -- what a viewer without granted access is shown --------------------------

  "toEntryImpl" should "serve the published name and description to a viewer without granted access" in {
    // Publishing pins what the public sees, and a listing is where the public meets a workflow, so
    // the listing has to serve the pinned copy too rather than the author's live metadata.
    val record = translatedRecord(
      grantedAccess = java.lang.Boolean.FALSE,
      publishedName = "as-published",
      publishedDescription = "described-as-published"
    )

    val workflow = workflowOf(record, viewerUid).workflow

    workflow.getName shouldBe "as-published"
    workflow.getDescription shouldBe "described-as-published"
  }

  it should "leave the author's live name and description for a viewer who was granted access" in {
    val record = translatedRecord(
      grantedAccess = java.lang.Boolean.TRUE,
      publishedName = "as-published",
      publishedDescription = "described-as-published"
    )

    val workflow = workflowOf(record, ownerUid).workflow

    workflow.getName shouldBe "wf-name"
    workflow.getDescription shouldBe "wf-description"
  }

  it should "treat an unknown access answer as public, which is the safe direction" in {
    // Showing the published copy to someone who turns out to have access is harmless; the reverse
    // would hand the author's live metadata to the public.
    val record = translatedRecord(grantedAccess = null, publishedName = "as-published")

    workflowOf(record, viewerUid).workflow.getName shouldBe "as-published"
  }

  it should "keep the author's name when a public workflow has nothing pinned" in {
    // Nothing to substitute, so the row is left as it is rather than blanked.
    val record = translatedRecord(grantedAccess = java.lang.Boolean.FALSE, publishedName = null)

    workflowOf(record, viewerUid).workflow.getName shouldBe "wf-name"
  }

  // -- the aggregate-field lookup --------------------------------------------

  "toEntryImpl" should "resolve the project aggregate through structural Field equality" in {
    // toEntryImpl builds a *fresh* groupConcatDistinct instance rather than
    // reusing the one in mappedResourceSchema, so the whole projects feature
    // rides on jOOQ treating two structurally identical aggregates as equal.
    val record = translatedRecord(projects = "5,6")
    JDSL.groupConcatDistinct(WORKFLOW_OF_PROJECT.PID) shouldBe pidField
    record.get(JDSL.groupConcatDistinct(WORKFLOW_OF_PROJECT.PID)) shouldBe "5,6"
  }

  // -- projectsOfWorkflow: both branches -------------------------------------

  it should "split the comma-joined aggregate into Integers, preserving the aggregate's order" in {
    // Deliberately unsorted so an accidental `.sorted` / `.reverse` fails.
    workflowOf(translatedRecord(projects = "3,1,2"), ownerUid).projectIDs shouldBe
      List(Integer.valueOf(3), Integer.valueOf(1), Integer.valueOf(2))
  }

  it should "handle a single-project aggregate (no separator present)" in {
    workflowOf(translatedRecord(projects = "8"), ownerUid).projectIDs shouldBe
      List(Integer.valueOf(8))
  }

  it should "return an empty project list when the aggregate is NULL" in {
    // A workflow that belongs to no project left-joins to a NULL aggregate.
    workflowOf(translatedRecord(projects = null), ownerUid).projectIDs shouldBe empty
  }

  // Deliberately NOT asserted: that a padded separator ("1, 2") or a leading comma
  // (",1") throws NumberFormatException. It does today — `Integer.valueOf` is applied
  // straight to the raw `split(',')` output, so such input aborts the whole search
  // request with a 500 instead of degrading. But production cannot produce it
  // (Postgres' string_agg is rendered with a bare ',' separator), and pinning the
  // throw would turn this suite red the moment someone hardens the parser with
  // `.map(_.trim).filter(_.nonEmpty)` — i.e. it would punish an improvement.

  // -- privilege fallback -----------------------------------------------------

  it should "fall back to NONE when the workflow privilege is NULL" in {
    // Public workflows the caller has no explicit grant on left-join to a NULL
    // privilege; the DTO must still carry a usable access level.
    workflowOf(translatedRecord(privilege = null), ownerUid).accessLevel shouldBe
      PrivilegeEnum.NONE.toString
  }

  it should "pass a non-NULL privilege through unchanged" in {
    // READ (not the WRITE default) so the fallback cannot be mistaken for a
    // pass-through of the fixture value.
    workflowOf(
      translatedRecord(privilege = PrivilegeEnum.READ),
      ownerUid
    ).accessLevel shouldBe "READ"
  }

  // -- ownership --------------------------------------------------------------

  it should "flag ownership by comparing workflow_of_user.uid against the caller" in {
    workflowOf(translatedRecord(), ownerUid).isOwner shouldBe true
    workflowOf(translatedRecord(), viewerUid).isOwner shouldBe false
  }

  it should "report the owner's id and name even when the caller is not the owner" in {
    val dw = workflowOf(translatedRecord(), viewerUid)
    dw.isOwner shouldBe false
    // `ownerId` is read as `record.into(USER).getUid`, but the unified schema
    // never selects USER.UID — jOOQ resolves the unqualified column name "uid"
    // against workflow_of_user.uid instead. So this must be the owner's 42,
    // neither the caller's 43 nor the workflow's wid of 7.
    dw.ownerId shouldBe ownerUid
    dw.ownerName shouldBe "owner-name"
  }

  // -- workflow payload + cover image ----------------------------------------

  it should "copy the workflow columns into the POJO without confusing them with the owner's" in {
    // The record carries two columns named "name" (workflow.name and
    // user.name); qualified resolution has to keep them apart.
    val dw = workflowOf(translatedRecord(), ownerUid)
    dw.workflow.getWid shouldBe wid
    dw.workflow.getName shouldBe "wf-name"
    dw.workflow.getDescription shouldBe "wf-description"
    dw.ownerName shouldBe "owner-name"
  }

  it should "wrap the cover image in an Option" in {
    workflowOf(translatedRecord(), ownerUid).coverImage shouldBe Some("cover-b64")
    workflowOf(translatedRecord(cover = null), ownerUid).coverImage shouldBe None
  }

  it should "tag the entry as a workflow and leave the other payload slots empty" in {
    val entry = WorkflowSearchQueryBuilder.toEntryImpl(ownerUid, translatedRecord())
    entry.resourceType shouldBe "workflow"
    entry.project shouldBe None
    entry.dataset shouldBe None
    entry.workflow should not be None
  }

  // -- the projection literal the dispatch depends on ------------------------

  "mappedResourceSchema" should "project the literal 'workflow' as the resourceType column" in {
    // searchAllResources dispatches on the *value* of this column with no
    // default branch, so changing the literal turns an "all resources" search
    // into a MatchError at fetch time rather than a compile error.
    // Rendered as a SELECT because an aliased Field on its own renders as the
    // bare alias reference, not as its underlying expression.
    val rendered = ctx.renderInlined(
      JDSL.select(WorkflowSearchQueryBuilder.mappedResourceSchema.allFields: _*)
    )
    rendered should include("'workflow' as \"resourceType\"")
    // The projects column is the aggregate toEntryImpl re-creates locally; a
    // separator other than a bare ',' would break the split above.
    rendered should include(s"""${ctx.renderInlined(pidField)} as "projects"""")
  }
}
