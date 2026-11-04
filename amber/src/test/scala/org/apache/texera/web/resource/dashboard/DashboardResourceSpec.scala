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

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.jooq.impl.{DSL => JDSL}
import org.jooq.{OrderField, SQLDialect}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.ws.rs.BadRequestException

/**
  * Covers the connection-free surface of [[DashboardResource]]: the `orderBy`
  * translation and the resource-type dispatch guard.
  *
  * Breakage this catches:
  *   - a column of the `orderBy` grammar wired to the wrong unified-schema
  *     alias (e.g. `EditTime` sorting by creation time), which silently
  *     re-orders the dashboard instead of failing;
  *   - the `nullsLast()` on the descending branch being dropped or copied onto
  *     the ascending branch — under Postgres that flips where rows with a NULL
  *     execution time land;
  *   - the regex losing its implicit anchoring (`Regex.unapplySeq` requires a
  *     full match), which would start accepting junk like `SortByNameAsc`;
  *   - an unrecognised `orderBy` no longer degrading to "no ORDER BY", or the
  *     unknown-resourceType guard no longer throwing before the query is built.
  *
  * Note on the shared global `FulltextSearchQueryUtils.usePgroonga`: nothing
  * exercised here reads or writes it, so this spec neither depends on its value
  * nor mutates it (a "restore" would itself be a write, and amber's suites
  * share one JVM).
  */
class DashboardResourceSpec extends AnyFlatSpec with Matchers {

  // jOOQ render context (Postgres dialect to match production renderers).
  // Dialect-only DSLContext — it never opens a connection.
  private val ctx = JDSL.using(SQLDialect.POSTGRES)

  private def sqlOf(o: OrderField[_]): String = ctx.renderInlined(o)

  private def orderSql(orderBy: String): List[String] =
    DashboardResource.getOrderFields(SearchQueryParams(orderBy = orderBy)).map(sqlOf)

  // -- getOrderFields: the recognised grammar ---------------------------------

  "getOrderFields" should "map each recognised column onto its unified-schema alias, ascending" in {
    // The rendered identifier is the alias of the UNION-ALL projection, not a
    // physical column: sorting happens after the three sub-queries are merged.
    orderSql("NameAsc") shouldBe List("\"resourceName\" asc")
    orderSql("CreateTimeAsc") shouldBe List("\"resourceCreationTime\" asc")
    orderSql("EditTimeAsc") shouldBe List("\"resourceLastModifiedTime\" asc")
    orderSql("ExecutionTimeAsc") shouldBe List("\"resourceExecutionTime\" asc")
  }

  it should "map each recognised column onto its unified-schema alias, descending" in {
    orderSql("NameDesc") shouldBe List("\"resourceName\" desc nulls last")
    orderSql("CreateTimeDesc") shouldBe List("\"resourceCreationTime\" desc nulls last")
    orderSql("EditTimeDesc") shouldBe List("\"resourceLastModifiedTime\" desc nulls last")
    orderSql("ExecutionTimeDesc") shouldBe List("\"resourceExecutionTime\" desc nulls last")
  }

  it should "spell out nulls-last only on the descending branch" in {
    // `Desc` goes through `value.desc().nullsLast()` while `Asc` is a bare
    // `value.asc()`. The asymmetry in the code is what makes the *semantics*
    // symmetric under Postgres, whose defaults are NULLS LAST for ASC and
    // NULLS FIRST for DESC: workflows that never ran (NULL executionTime)
    // therefore sort to the end in both directions. Removing `nullsLast()`
    // would quietly float them to the top of the descending listing.
    val asc = orderSql("ExecutionTimeAsc").head
    val desc = orderSql("ExecutionTimeDesc").head
    asc should not include "nulls"
    desc should include("nulls last")
  }

  it should "emit exactly one ORDER BY term" in {
    // A second term would change pagination stability; the contract is one
    // field per request.
    DashboardResource.getOrderFields(
      SearchQueryParams(orderBy = "CreateTimeDesc")
    ) should have size 1
  }

  // -- getOrderFields: everything the grammar rejects -------------------------

  it should "fall back to no ordering at all when orderBy does not match the pattern" in {
    // Consequence of the empty list: the query runs unordered while
    // offset/limit still apply, so pagination silently becomes
    // non-deterministic. Pin the inputs that take this branch.
    orderSql("") shouldBe empty // the frontend can send an empty query param
    orderSql("Bogus") shouldBe empty
    orderSql("NameSideways") shouldBe empty // known column, junk direction
    orderSql("Name") shouldBe empty // direction missing entirely
    orderSql("nameAsc") shouldBe empty // the regex is case-sensitive
    orderSql("SortByNameAsc") shouldBe empty // Regex.unapplySeq anchors the
    orderSql("NameAscending") shouldBe empty // whole string, both ends
  }

  // Two branches in this area are unreachable and are therefore deliberately
  // NOT tested: `case None => List()` inside getOrderFields and
  // `case _ => null` inside the private getColumnField. The regex can only
  // yield the four column names Name/CreateTime/EditTime/ExecutionTime, and
  // every one of them maps to a non-null Field, so getColumnField never
  // returns None.

  // -- searchAllResources: the dispatch guard --------------------------------

  "searchAllResources" should "reject an unknown resourceType before it builds any query" in {
    // The `case _ => throw` arm sits in the dispatch match, ahead of
    // constructQuery / SqlServer, so this is reachable with no database. The
    // exception *type* is the real assertion: if the guard were moved below the
    // query construction, this would surface as a SqlServer failure instead.
    val thrown = the[BadRequestException] thrownBy DashboardResource.searchAllResources(
      new SessionUser(new User()), // uid stays null; unused on this path
      SearchQueryParams(resourceType = "notAResourceType")
    )
    thrown.getMessage should include("notAResourceType")
  }

  it should "reject a resourceType that only differs from a valid one by case" in {
    // Guards against someone relaxing the match to be case-insensitive on one
    // side only: "Workflow" is not "workflow" today.
    val thrown = the[BadRequestException] thrownBy DashboardResource.searchAllResources(
      new SessionUser(new User()),
      SearchQueryParams(resourceType = "Workflow")
    )
    thrown.getMessage should include("Workflow")
  }
}
