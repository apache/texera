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

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
  * Covers the access-control shape of the SQL `DatasetSearchQueryBuilder` produces.
  *
  * Every member of the builder is `override protected`, so unlike the workflow and project arms
  * nothing here is directly callable. The one public route in is the trait's `final constructQuery`,
  * and what it returns can be rendered to SQL and inspected without ever executing it — so these are
  * assertions about the query's *shape*, not about rows.
  *
  * What that buys is the piece of this file with real consequences: which datasets a caller is
  * allowed to see. Three branches decide it and none is otherwise tested —
  *
  *   - the join predicate `.and(if (uid == null) DSL.falseCondition() else DATASET_USER_ACCESS.UID.eq(uid))`,
  *     which scopes the grant rows to the caller. Without the `eq(uid)` the `UID.isNotNull` check
  *     below matches *anyone's* grant row, which is a complete sharing bypass;
  *   - the anonymous arm, which must restrict to public datasets and nothing else;
  *   - `includePublic == false`, which must return only explicitly-granted datasets and must not
  *     leak public ones in.
  *
  * `initializeDBAndReplaceDSLContext` is needed only because `SearchQueryBuilder.context` reads
  * `SqlServer.getInstance()`; no query is run against the database.
  *
  * Deliberately not covered: `toEntryImpl` (lines 128-164) is ~80% of this file's uncovered lines
  * and sits behind a live LakeFS `retrieveRepositorySize` call with no mockable seam. Reaching it
  * would need a source change (injecting a repository-size provider), not a test.
  */
class DatasetSearchQueryBuilderSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val uid: Integer = Integer.valueOf(42)

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  private def params(keywords: String*): SearchQueryParams =
    SearchQueryParams(keywords = keywords.toList.asJava)

  /**
    * The query rendered with its bind values inlined, lower-cased and with the identifier quoting
    * stripped, so assertions read as `dataset.is_public` rather than `"texera_db"."dataset"."is_public"`.
    */
  private def sqlFor(
      callerUid: Integer,
      includePublic: Boolean,
      p: SearchQueryParams = params()
  ): String =
    getDSLContext
      .renderInlined(DatasetSearchQueryBuilder.constructQuery(callerUid, p, includePublic))
      .toLowerCase
      .replace("\"", "")

  "an anonymous search" should "be restricted to public datasets" in {
    val sql = sqlFor(null, includePublic = true)

    sql should include("dataset.is_public = true")
    sql should not include "dataset_user_access.uid is not null"
  }

  it should "match no grant rows at all" in {
    // The join is still written, but its predicate is a constant false, so an anonymous caller can
    // never pick up somebody else's access row.
    val sql = sqlFor(null, includePublic = true)

    sql should include("false")
    sql should not include "dataset_user_access.uid = "
  }

  it should "ignore includePublic, which only applies to a signed-in caller" in {
    sqlFor(null, includePublic = true) shouldBe sqlFor(null, includePublic = false)
  }

  "a signed-in search" should "scope the grant join to that caller" in {
    // Without this predicate the `uid is not null` test below is satisfied by ANY user's grant row,
    // which would hand the caller every shared dataset in the system.
    val sql = sqlFor(uid, includePublic = false)

    sql should include(s"dataset_user_access.uid = $uid")
  }

  it should "return only explicitly granted datasets when public ones are excluded" in {
    val sql = sqlFor(uid, includePublic = false)

    sql should include("dataset_user_access.uid is not null")
    sql should not include "is_public = true"
  }

  it should "add public datasets to the granted ones when they are included" in {
    val sql = sqlFor(uid, includePublic = true)

    sql should include("dataset.is_public = true")
    sql should include("dataset_user_access.uid is not null")
    sql should include(" or ")
  }

  "the keyword filter" should "split on the punctuation the full-text engine reserves" in {
    // A user pasting `a+b` means two terms, not a literal. The split set is this file's own and is
    // not shared with the other builders.
    val sql = sqlFor(uid, includePublic = true, params("alpha+beta"))

    sql should include("alpha")
    sql should include("beta")
    sql should not include "alpha+beta"
  }

  "the query" should "dedupe with selectDistinct rather than a group by" in {
    // getGroupByFields is empty here, unlike the workflow and project builders, so the DISTINCT is
    // the only thing collapsing the rows the access join multiplies out.
    val sql = sqlFor(uid, includePublic = true)

    sql should include("select distinct")
    sql should not include "group by"
  }

  it should "tag its rows as datasets" in {
    // DashboardResource dispatches on this literal with no default branch, so a change here is a
    // MatchError at fetch time rather than a compile error.
    sqlFor(uid, includePublic = true) should include("'dataset'")
  }
}
