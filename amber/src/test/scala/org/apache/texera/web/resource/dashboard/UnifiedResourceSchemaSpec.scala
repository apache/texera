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
import org.jooq.impl.{DSL => JDSL}
import org.jooq.{Field, SQLDialect}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

/**
  * Covers `UnifiedResourceSchema.apply` — the projection every search builder
  * unions into — and the de-duplication it performs behind `allFields`.
  *
  * Breakage this catches:
  *   - a projection slot wired to the wrong alias. Note the builders cannot drift
  *     from each other: the class constructor is private and every builder goes
  *     through this one `apply`, which pairs each expression with its own alias in
  *     the same tuple. So the slot-order assertion here is a lock on a centrally
  *     fixed projection shape (which `searchAllResources` unions positionally),
  *     not protection against one builder reordering columns — that is impossible;
  *   - the de-dup in `translatedFieldSet` changing from "keep the first alias
  *     per distinct original Field" to keep-last (or disappearing), which
  *     changes which alias `translateRecord` reads values out of;
  *   - jOOQ no longer comparing Fields structurally, which is the sole reason
  *     the de-dup collapses anything at all.
  *
  * `translateRecord` itself is out of scope: it calls `context.newRecord`, and
  * `context` is `SqlServer.getInstance().createDSLContext()`, so exercising it
  * would need a live database. Nothing here reads or writes the shared global
  * `FulltextSearchQueryUtils.usePgroonga`.
  */
class UnifiedResourceSchemaSpec extends AnyFlatSpec with Matchers {

  // jOOQ render context (Postgres dialect to match production renderers).
  private val ctx = JDSL.using(SQLDialect.POSTGRES)

  // `translatedFieldSet` is a `private val` whose only public consumer
  // (translateRecord) needs a database, so read it reflectively rather than
  // standing one up. Keeping this observable is the point: the de-dup is what
  // decides which aliases ever reach a translated record.
  private val translatedFieldSetAccessor = {
    val field = classOf[UnifiedResourceSchema].getDeclaredField("translatedFieldSet")
    field.setAccessible(true)
    field
  }

  private def translatedPairs(schema: UnifiedResourceSchema): Seq[(Field[_], Field[_])] =
    translatedFieldSetAccessor
      .get(schema)
      .asInstanceOf[Seq[(Field[_], Field[_])]]

  private def translatedAliases(schema: UnifiedResourceSchema): Seq[String] =
    translatedPairs(schema).map(_._2.getName)

  private def translatedOriginals(schema: UnifiedResourceSchema): Seq[Field[_]] =
    translatedPairs(schema).map(_._1)

  // Sentinels for the three slots that have no convenient distinct table
  // column of the right type; every other slot uses a real generated column so
  // that all 28 originals render differently from one another.
  private val sentinelResourceType: Field[String] = JDSL.inline("s-resource-type")
  private val sentinelProjects: Field[String] = JDSL.inline("s-projects")
  private val sentinelStoragePath: Field[String] = JDSL.inline("s-storage-path")
  private val sentinelGranted: Field[java.lang.Boolean] = JDSL.inline(java.lang.Boolean.TRUE)

  private val sentinelSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = sentinelResourceType,
    name = WORKFLOW.NAME,
    description = DATASET.DESCRIPTION,
    creationTime = WORKFLOW.CREATION_TIME,
    lastModifiedTime = WORKFLOW.LAST_MODIFIED_TIME,
    executionTime = WORKFLOW_EXECUTIONS.STARTING_TIME,
    ownerId = WORKFLOW_OF_USER.UID,
    wid = WORKFLOW.WID,
    workflowUserAccess = WORKFLOW_USER_ACCESS.PRIVILEGE,
    projectsOfWorkflow = sentinelProjects,
    uid = USER.UID,
    userName = USER.NAME,
    userEmail = USER.EMAIL,
    pid = PROJECT.PID,
    projectOwnerId = PROJECT.OWNER_ID,
    projectColor = PROJECT.COLOR,
    did = DATASET.DID,
    datasetStoragePath = sentinelStoragePath,
    repositoryName = DATASET.REPOSITORY_NAME,
    isDatasetPublic = DATASET.IS_PUBLIC,
    isDatasetDownloadable = DATASET.IS_DOWNLOADABLE,
    datasetUserAccess = DATASET_USER_ACCESS.PRIVILEGE,
    datasetCoverImage = DATASET.COVER_IMAGE,
    workflowCoverImage = WORKFLOW_COVER_IMAGE.IMAGE,
    workflowHasUnpublishedChanges = WORKFLOW.IS_PUBLIC,
    workflowPublishedName = WORKFLOW.PUBLISHED_NAME,
    workflowPublishedDescription = WORKFLOW.PUBLISHED_DESCRIPTION,
    viewerHasGrantedAccess = sentinelGranted
  )

  // Expected projection, in order: alias -> the original it must be built from.
  private val expectedProjection: Seq[(String, Field[_])] = Seq(
    "resourceType" -> sentinelResourceType,
    "resourceName" -> WORKFLOW.NAME,
    "resourceDescription" -> DATASET.DESCRIPTION,
    "resourceCreationTime" -> WORKFLOW.CREATION_TIME,
    "resourceLastModifiedTime" -> WORKFLOW.LAST_MODIFIED_TIME,
    "resourceExecutionTime" -> WORKFLOW_EXECUTIONS.STARTING_TIME,
    "resourceOwnerId" -> WORKFLOW_OF_USER.UID,
    "wid" -> WORKFLOW.WID,
    "workflow_privilege" -> WORKFLOW_USER_ACCESS.PRIVILEGE,
    "projects" -> sentinelProjects,
    "uid" -> USER.UID,
    "userName" -> USER.NAME,
    "email" -> USER.EMAIL,
    "pid" -> PROJECT.PID,
    "owner_uid" -> PROJECT.OWNER_ID,
    "color" -> PROJECT.COLOR,
    "did" -> DATASET.DID,
    "dataset_storage_path" -> sentinelStoragePath,
    "repository_name" -> DATASET.REPOSITORY_NAME,
    "is_dataset_public" -> DATASET.IS_PUBLIC,
    "is_dataset_downloadable" -> DATASET.IS_DOWNLOADABLE,
    "user_dataset_access" -> DATASET_USER_ACCESS.PRIVILEGE,
    "cover_image" -> DATASET.COVER_IMAGE,
    "workflow_cover_image" -> WORKFLOW_COVER_IMAGE.IMAGE,
    "workflow_has_unpublished_changes" -> WORKFLOW.IS_PUBLIC,
    "workflow_published_name" -> WORKFLOW.PUBLISHED_NAME,
    "workflow_published_description" -> WORKFLOW.PUBLISHED_DESCRIPTION,
    "viewer_has_granted_access" -> sentinelGranted
  )

  // -- apply(): the projection ------------------------------------------------

  "apply" should "expose all 28 slots as aliases, in the order the UNION ALL depends on" in {
    sentinelSchema.allFields should have size 28
    sentinelSchema.allFields.map(_.getName) shouldBe expectedProjection.map(_._1)
  }

  it should "alias each original to its own slot" in {
    // Rendered as a SELECT because that is how constructQuery consumes
    // allFields; a swapped pair (`name -> description.as(resourceNameAlias)`)
    // shows up here even though the alias order stays intact.
    val rendered = ctx.renderInlined(JDSL.select(sentinelSchema.allFields: _*))
    expectedProjection.foreach {
      case (alias, original) =>
        rendered should include(s"""${ctx.renderInlined(original)} as "$alias"""")
    }
  }

  it should "default every slot to an inline literal or a typed NULL cast" in {
    // The all-defaults projection is what lets a builder that knows nothing
    // about datasets still union with one that does: the column count and
    // types have to line up.
    val defaults = UnifiedResourceSchema()
    defaults.allFields should have size 28
    val rendered = ctx.renderInlined(JDSL.select(defaults.allFields: _*))
    rendered should include("'' as \"resourceType\"")
    rendered should include("cast(null as timestamp) as \"resourceCreationTime\"")
    rendered should include("cast(null as int) as \"resourceOwnerId\"")
    rendered should include("cast(null as boolean) as \"is_dataset_public\"")
  }

  // -- the de-dup behind translatedFieldSet -----------------------------------

  "translatedFieldSet" should "keep the first alias when the same Field feeds two slots" in {
    // WorkflowSearchQueryBuilder really does this: `ownerId` and `uid` are both
    // WORKFLOW_OF_USER.UID. allFields keeps both aliases (the SELECT projects the
    // column twice) while the translation map keeps only the earlier one.
    //
    // What that does and does not mean: `translateRecord` builds its output with
    // `context.newRecord(translatedFieldSet.map(_._1): _*)`, i.e. keyed by the
    // ORIGINAL fields, so no `uid`-named slot exists on a translated record either
    // way. And since both slots project the *same* expression, the dropped alias
    // would have carried the same value — no data is lost. The only consequence is
    // that the source record's `uid` alias column is never read back. So this test
    // pins the keep-first rule itself, not a data loss.
    val shared: Field[Integer] = JDSL.field(JDSL.name("shared_uid"), classOf[Integer])
    val schema = UnifiedResourceSchema(ownerId = shared, uid = shared)

    val aliases = translatedAliases(schema)
    aliases should contain("resourceOwnerId") // first occurrence wins
    aliases should not contain "uid" // second occurrence is dropped
    // The de-dup rule is about the ORIGINALS: one entry per distinct original Field.
    val originals = translatedOriginals(schema)
    originals.distinct.size shouldBe originals.size
    originals.count(_ == shared) shouldBe 1
  }

  it should "collapse the all-defaults projection down to one alias per distinct default" in {
    // 28 slots, but only six structurally distinct default expressions, so the
    // de-dup collapses the map to six entries. Worth pinning because it is
    // surprising, and because it is what makes the keep-first rule observable at
    // all: allFields stays at 28 while the translation map does not.
    val defaults = UnifiedResourceSchema()
    defaults.allFields should have size 28
    translatedAliases(defaults) shouldBe Seq(
      "resourceType", // DSL.inline("")
      "resourceCreationTime", // cast(null as timestamp)
      "resourceOwnerId", // cast(null as int)
      "workflow_privilege", // cast(null as privilege_enum)
      "dataset_storage_path", // cast(null as varchar)
      "is_dataset_public" // cast(null as boolean)
    )
  }

  it should "keep every distinct original when the caller supplies 28 distinct Fields" in {
    // Nothing to collapse here, which is the control case for the two tests
    // above: the shrinkage they observe comes from duplicate originals only.
    translatedAliases(sentinelSchema) shouldBe expectedProjection.map(_._1)
  }

  it should "drop exactly the duplicated slots of the production workflow projection" in {
    val workflowSchema = WorkflowSearchQueryBuilder.mappedResourceSchema
    workflowSchema.allFields should have size 28
    val aliases = translatedAliases(workflowSchema)
    // `uid` duplicates ownerId (WORKFLOW_OF_USER.UID); the rest are slots the
    // builder left at their default, and the defaults collide by type.
    workflowSchema.allFields.map(_.getName).diff(aliases) shouldBe Seq(
      "uid",
      "owner_uid",
      "color",
      "did",
      "repository_name",
      "is_dataset_downloadable",
      "cover_image"
    )
    aliases should contain("resourceOwnerId")
  }

  // -- the jOOQ assumption the de-dup rests on -------------------------------

  "jOOQ Field equality" should "be structural, which is what makes the de-dup collapse anything" in {
    // If jOOQ ever switched to identity equality, translatedFieldSet would keep
    // all 28 slots and translateRecord would start reading duplicated columns —
    // the tests above would flip, and this one says why.
    JDSL.cast(null, classOf[Integer]) shouldBe JDSL.cast(null, classOf[Integer])
    JDSL.inline("") shouldBe JDSL.inline("")
    // ...but only within a type: the six default expressions stay distinct.
    JDSL.cast(null, classOf[Integer]) should not be JDSL.cast(null, classOf[Timestamp])
    JDSL.cast(null, classOf[String]) should not be JDSL.castNull(classOf[PrivilegeEnum])
  }
}
