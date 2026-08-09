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

  private val coverField = JDSL.max(WORKFLOW_COVER_IMAGE.IMAGE).as("workflow_cover_image")

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
      cover: String = "cover-b64"
  ): Record = {
    val record = ctx.newRecord(
      WORKFLOW.WID,
      WORKFLOW.NAME,
      WORKFLOW.DESCRIPTION,
      WORKFLOW_OF_USER.UID,
      WORKFLOW_USER_ACCESS.PRIVILEGE,
      USER.NAME,
      coverField
    )
    record.set(WORKFLOW.WID, wid)
    record.set(WORKFLOW.NAME, "wf-name")
    record.set(WORKFLOW.DESCRIPTION, "wf-description")
    record.set(WORKFLOW_OF_USER.UID, uidValue)
    record.set(WORKFLOW_USER_ACCESS.PRIVILEGE, privilege)
    record.set(USER.NAME, "owner-name")
    record.set(coverField, cover)
    record
  }

  private def workflowOf(record: Record, uid: Integer): DashboardWorkflow =
    WorkflowSearchQueryBuilder.toEntryImpl(uid, record).workflow.get

  // -- privilege fallback -----------------------------------------------------

  "toEntryImpl" should "fall back to NONE when the workflow privilege is NULL" in {
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
  }
}
