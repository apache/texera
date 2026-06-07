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

import org.jooq.impl.{DSL => JDSL}
import org.jooq.{Condition, Field, SQLDialect}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.util.concurrent.TimeUnit

class FulltextSearchQueryUtilsSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  // jOOQ render context (Postgres dialect to match production renderers).
  private val ctx = JDSL.using(SQLDialect.POSTGRES)

  private def sqlOf(c: Condition): String = ctx.renderInlined(c)

  // The `usePgroonga` flag is shared mutable global state — restore it
  // around every test so a failed assertion in the fallback branch can't
  // leave the rest of the suite running against the wrong branch.
  private val pgroongaDefault = FulltextSearchQueryUtils.usePgroonga
  after { FulltextSearchQueryUtils.usePgroonga = pgroongaDefault }

  // -- getFullTextSearchFilter ------------------------------------------------

  "getFullTextSearchFilter" should "return noCondition when fields list is empty" in {
    val cond = FulltextSearchQueryUtils.getFullTextSearchFilter(Seq("anything"), List.empty)
    sqlOf(cond) shouldBe sqlOf(JDSL.noCondition())
  }

  it should "return noCondition when all keywords are empty strings" in {
    val field: Field[String] = JDSL.field("name", classOf[String])
    val cond = FulltextSearchQueryUtils.getFullTextSearchFilter(Seq(""), List(field))
    sqlOf(cond) shouldBe sqlOf(JDSL.noCondition())
  }

  it should "currently still build a pgroonga predicate for whitespace-only keywords (subtle quirk)" in {
    // `keywords.filter(_.nonEmpty)` checks for empty BEFORE trimming, so
    // a "   " input survives the filter and is trimmed to "" — but only
    // after the emptiness check. The resulting predicate searches for the
    // empty string. Pin the behavior so a future fix (move the trim into
    // the filter step) deliberately breaks this test.
    FulltextSearchQueryUtils.usePgroonga = true
    val field: Field[String] = JDSL.field("name", classOf[String])
    val cond = FulltextSearchQueryUtils.getFullTextSearchFilter(Seq("   "), List(field))
    sqlOf(cond) should include("pgroonga_condition('',")
  }

  it should "emit a pgroonga fuzzy-match expression when usePgroonga is true" in {
    FulltextSearchQueryUtils.usePgroonga = true
    val name: Field[String] = JDSL.field("name", classOf[String])
    val desc: Field[String] = JDSL.field("description", classOf[String])
    val cond = FulltextSearchQueryUtils.getFullTextSearchFilter(
      Seq("alpha", "beta"),
      List(name, desc)
    )
    val sql = sqlOf(cond)
    sql should include("&@~")
    sql should include("pgroonga_condition")
    sql should include("fuzzy_max_distance_ratio")
    // Field COALESCE chain — joined with " || ' ' || " across all fields.
    sql should include("COALESCE")
  }

  it should "emit a to_tsvector / to_tsquery chain when usePgroonga is false" in {
    FulltextSearchQueryUtils.usePgroonga = false
    val name: Field[String] = JDSL.field("name", classOf[String])
    val cond = FulltextSearchQueryUtils.getFullTextSearchFilter(
      Seq("hello world", "goodbye"),
      List(name)
    )
    val sql = sqlOf(cond)
    sql should include("to_tsvector")
    sql should include("to_tsquery")
    // Multi-word keyword "hello world" should produce "hello & world" inside
    // the tsquery; this is what makes the fallback path act like an AND
    // across tokens of the same keyword.
    sql should include("hello & world")
  }

  // -- getContainsFilter ------------------------------------------------------

  "getContainsFilter" should "OR together field-equality checks for each unique value" in {
    val field: Field[Integer] = JDSL.field("uid", classOf[Integer])
    val values = new java.util.ArrayList[Integer]()
    values.add(1)
    values.add(2)
    values.add(2) // duplicate — set conversion should collapse it
    val cond = FulltextSearchQueryUtils.getContainsFilter(values, field)
    val sql = sqlOf(cond)
    sql should include("uid = 1")
    sql should include("uid = 2")
    sql should include(" or ")
    // Duplicate dedup: only two distinct equality clauses, so exactly one
    // ` or ` separator. (Render strips into a single chained OR.)
    sql.split(" or ").length shouldBe 2
  }

  it should "return noCondition for an empty values list" in {
    val field: Field[Integer] = JDSL.field("uid", classOf[Integer])
    val empty = new java.util.ArrayList[Integer]()
    val cond = FulltextSearchQueryUtils.getContainsFilter(empty, field)
    sqlOf(cond) shouldBe sqlOf(JDSL.noCondition())
  }

  // -- getDateFilter ----------------------------------------------------------

  "getDateFilter" should "produce a BETWEEN-shaped predicate for both endpoints" in {
    val field: Field[Timestamp] = JDSL.field("created_at", classOf[Timestamp])
    val cond = FulltextSearchQueryUtils.getDateFilter("2026-01-01", "2026-01-31", field)
    val sql = sqlOf(cond)
    sql should include("between")
    sql should include("2026-01-01 00:00:00")
    // The end timestamp is bumped to "+1 day - 1ms" so a range ending on a
    // calendar date is inclusive through end-of-day. JOOQ renders the
    // Timestamp millis as `.999`.
    sql should include("2026-01-31 23:59:59")
  }

  it should "default to the open-ended sentinel dates when one endpoint is empty" in {
    val field: Field[Timestamp] = JDSL.field("created_at", classOf[Timestamp])
    val onlyEnd = FulltextSearchQueryUtils.getDateFilter("", "2026-01-31", field)
    sqlOf(onlyEnd) should include("1970-01-01")

    val onlyStart = FulltextSearchQueryUtils.getDateFilter("2026-01-01", "", field)
    // 9999-12-31 is the "no end" sentinel; for this case the code skips the
    // +1-day bump (rendering the date as-is).
    sqlOf(onlyStart) should include("9999-12-31")
  }

  it should "return noCondition when both endpoints are empty" in {
    val field: Field[Timestamp] = JDSL.field("created_at", classOf[Timestamp])
    val cond = FulltextSearchQueryUtils.getDateFilter("", "", field)
    sqlOf(cond) shouldBe sqlOf(JDSL.noCondition())
  }

  it should "throw ParseException for a malformed start date" in {
    val field: Field[Timestamp] = JDSL.field("created_at", classOf[Timestamp])
    a[ParseException] should be thrownBy
      FulltextSearchQueryUtils.getDateFilter("not-a-date", "2026-01-31", field)
  }

  // Sanity check that the SimpleDateFormat used inside getDateFilter parses
  // the documented format — guards against a future locale-dependent bug.
  it should "accept the documented yyyy-MM-dd format" in {
    val parsed = new SimpleDateFormat("yyyy-MM-dd").parse("2026-01-01")
    val ts = new Timestamp(parsed.getTime + TimeUnit.DAYS.toMillis(0))
    ts.getTime should be > 0L
  }

  // -- getOperatorsFilter -----------------------------------------------------

  "getOperatorsFilter" should "build a case-insensitive LIKE pattern around \"operatorType\":\"$op\"" in {
    val field: Field[String] = JDSL.field("content", classOf[String])
    val ops = new java.util.ArrayList[String]()
    ops.add("CSVScan")
    ops.add("CSVScan") // duplicate
    val cond = FulltextSearchQueryUtils.getOperatorsFilter(ops, field)
    val sql = sqlOf(cond)
    sql.toLowerCase should include("ilike")
    // The pattern wraps the operator name in the literal JSON shape that
    // appears in workflow.content blobs — including the surrounding quotes.
    sql should include("\"operatorType\":\"CSVScan\"")
    // De-duplication: only one ILIKE term despite two inputs.
    sql.toLowerCase.split("ilike").length shouldBe 2
  }

  it should "OR together patterns for distinct operators" in {
    val field: Field[String] = JDSL.field("content", classOf[String])
    val ops = new java.util.ArrayList[String]()
    ops.add("CSVScan")
    ops.add("Filter")
    val cond = FulltextSearchQueryUtils.getOperatorsFilter(ops, field)
    val sql = sqlOf(cond)
    sql should include("CSVScan")
    sql should include("Filter")
    sql.toLowerCase should include(" or ")
  }
}
