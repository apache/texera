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

import org.apache.texera.dao.jooq.generated.Tables.PROJECT
import org.apache.texera.dao.jooq.generated.tables.pojos.Project
import org.jooq.impl.{DSL => JDSL}
import org.jooq.{Record, SQLDialect}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class ProjectSearchQueryBuilderSpec extends AnyFlatSpec with Matchers {

  private val ctx = JDSL.using(SQLDialect.POSTGRES)

  private val ownerUid: Integer = Integer.valueOf(42)
  private val callerUid: Integer = Integer.valueOf(43)
  private val pid: Integer = Integer.valueOf(7)
  private val createdAt = new Timestamp(1700000000123L)

  // In-memory record shaped like the one toEntryImpl receives (keyed by the
  // original PROJECT fields). Values are distinct so a wrong-column read fails.
  private def translatedRecord(
      description: String = "proj-description",
      color: String = "aabbcc"
  ): Record = {
    val record = ctx.newRecord(
      PROJECT.PID,
      PROJECT.NAME,
      PROJECT.DESCRIPTION,
      PROJECT.OWNER_ID,
      PROJECT.CREATION_TIME,
      PROJECT.COLOR
    )
    record.set(PROJECT.PID, pid)
    record.set(PROJECT.NAME, "proj-name")
    record.set(PROJECT.DESCRIPTION, description)
    record.set(PROJECT.OWNER_ID, ownerUid)
    record.set(PROJECT.CREATION_TIME, createdAt)
    record.set(PROJECT.COLOR, color)
    record
  }

  private def projectOf(record: Record, uid: Integer): Project =
    ProjectSearchQueryBuilder.toEntryImpl(uid, record).project.get

  "toEntryImpl" should "copy every project column into the POJO" in {
    val p = projectOf(translatedRecord(), ownerUid)
    p.getPid shouldBe pid
    p.getName shouldBe "proj-name"
    p.getDescription shouldBe "proj-description"
    p.getOwnerId shouldBe ownerUid
    p.getCreationTime shouldBe createdAt
    p.getColor shouldBe "aabbcc"
  }

  it should "pass a NULL description and a NULL color through as null" in {
    // description and color are the only nullable project columns.
    val p = projectOf(translatedRecord(description = null, color = null), ownerUid)
    p.getDescription shouldBe null
    p.getColor shouldBe null
    p.getPid shouldBe pid
  }

  it should "tag the entry as a project and leave the other payload slots empty" in {
    // searchAllResources matches on resourceType with no default branch, so a
    // wrong tag is a runtime MatchError.
    val entry = ProjectSearchQueryBuilder.toEntryImpl(ownerUid, translatedRecord())
    entry.resourceType shouldBe "project"
    entry.project should not be None
    entry.workflow shouldBe None
    entry.dataset shouldBe None
  }

  it should "produce the same entry regardless of the caller's uid" in {
    // Unlike the workflow arm there is no ownership flag to compute.
    val record = translatedRecord()
    ProjectSearchQueryBuilder.toEntryImpl(ownerUid, record) shouldBe
      ProjectSearchQueryBuilder.toEntryImpl(callerUid, record)
  }

  // An aliased field renders as the bare alias on its own, so render a SELECT.
  private lazy val renderedSchema: String = ctx.renderInlined(
    JDSL.select(ProjectSearchQueryBuilder.mappedResourceSchema.allFields: _*)
  )

  "mappedResourceSchema" should "project the literal 'project' as the resourceType column" in {
    renderedSchema should include("'project' as \"resourceType\"")
  }

  it should "alias PROJECT.CREATION_TIME as both the creation and last-modified time" in {
    // Deliberate: the project table has no last-modified column. Without this
    // alias projects would NULL-sink in every sort by edit time.
    val creationTime = ctx.renderInlined(PROJECT.CREATION_TIME)
    renderedSchema should include(s"""$creationTime as "resourceCreationTime"""")
    renderedSchema should include(s"""$creationTime as "resourceLastModifiedTime"""")
  }

  it should "project PROJECT.COLOR as the color column" in {
    // The frontend reads the project colour swatch from this alias.
    val color = ctx.renderInlined(PROJECT.COLOR)
    renderedSchema should include(s"""$color as "color"""")
  }

  it should "project the project id and owner through the shared slots" in {
    // The owner rides the generic resourceOwnerId slot; the project-specific
    // "owner_uid" slot stays NULL and is deliberately not pinned here.
    renderedSchema should include(s"""${ctx.renderInlined(PROJECT.PID)} as "pid"""")
    renderedSchema should include(
      s"""${ctx.renderInlined(PROJECT.OWNER_ID)} as "resourceOwnerId""""
    )
  }

  it should "project the name and description through the shared full-text slots" in {
    // Also the two columns the keyword filter targets.
    renderedSchema should include(s"""${ctx.renderInlined(PROJECT.NAME)} as "resourceName"""")
    renderedSchema should include(
      s"""${ctx.renderInlined(PROJECT.DESCRIPTION)} as "resourceDescription""""
    )
  }
}
