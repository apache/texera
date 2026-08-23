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

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, DatasetUserAccessDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetUserAccess, User}
import org.apache.texera.web.resource.dashboard.DashboardResource.{
  DashboardClickableFileEntry,
  SearchQueryParams
}
import org.jooq.Record
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ExecutorService, Executors}
import scala.jdk.CollectionConverters._

/**
  * Covers `DatasetSearchQueryBuilder` from both ends: the shape of the SQL it builds, and the
  * `DashboardClickableFileEntry` it turns a fetched row into.
  *
  * == Part 1: which datasets a caller may see ==
  *
  * Every member of the builder is `override protected`, so the query side is reachable only through
  * the trait's `final constructQuery`, and what it returns can be rendered to SQL and inspected
  * without executing it — those are assertions about the query's *shape*. Three branches decide
  * visibility and nothing else tests them:
  *
  *   - the join predicate `.and(if (uid == null) DSL.falseCondition() else DATASET_USER_ACCESS.UID.eq(uid))`,
  *     which scopes the grant rows to the caller. Without the `eq(uid)` the `UID.isNotNull` check
  *     below matches *anyone's* grant row, which is a complete sharing bypass;
  *   - the anonymous arm, which must restrict to public datasets and nothing else;
  *   - `includePublic == false`, which must return only explicitly-granted datasets and must not
  *     leak public ones in.
  *
  * == Part 2: what a row becomes ==
  *
  * `toEntryImpl` is `override protected` here (the workflow and project siblings widen theirs to
  * `override def`), so it is not directly callable. The route in is the trait's public `toEntry`,
  * which first runs `UnifiedResourceSchema.translateRecord` — so the record handed to it must carry
  * the *aliased* select fields, not raw `DATASET.*` fields. Rather than hand-build that record,
  * these tests fetch the builder's own query and pass a real row, which is exactly what
  * `DashboardResource.searchAllResources` does.
  *
  * A record permutation is invisible from this end. `translateRecord` writes each aliased value back
  * to the very column the schema paired it with (`UnifiedResourceSchema:174`), so ANY swap of two
  * same-typed schema assignments — `name`/`description`, `is_public`/`is_downloadable`,
  * `did`/`ownerId` — cancels out exactly and produces a byte-identical entry. What the entry-level
  * column assertions below really pin is that each slot still references *some* `DATASET` column
  * (wire a slot to a literal and `translateRecord` drops the field, so the value reads back null).
  * The projections are pinned where a permutation is actually visible: the SQL-shape test
  * "project every dataset column under the alias its schema slot names".
  *
  * `toEntryImpl` also calls `LakeFSStorageClient.retrieveRepositorySize`, which is a live HTTP call
  * with no injectable seam. Two non-obvious things make it testable anyway:
  *
  *   - **The stub binds AT the configured endpoint** (`StorageConfig.lakefsEndpoint`, i.e.
  *     `localhost:8000`) instead of repointing that endpoint at an ephemeral port. This is
  *     deliberate. `LakeFSStorageClient.apiClient` is a `lazy val` that captures the endpoint once
  *     per JVM, and `amber/build.sbt` runs every suite in ONE unforked JVM
  *     (`Tags.limit(Tags.Test, 1)` serializes them but does not isolate them). Mutating the global
  *     `StorageConfig.lakefsEndpoint` here would therefore be order-dependent — any earlier suite
  *     that forced `apiClient` wins — and would leave the client bound to a dead port for every
  *     later suite. Binding at the address the config already names is order-independent and
  *     memoizes nothing wrongly. The price is that the port is machine-global: if something else
  *     holds it (a local `bin/local-dev.sh up` lakeFS, or a sibling worktree running these tests),
  *     the four tests that need a *successful* size `cancel` rather than fail. CI has no lakeFS in
  *     this job, so it is deterministic there.
  *
  *     A cancel is louder than it looks, and the sbt log shows it only as `canceled 4` while the
  *     build stays green. It disarms the whole `toEntry` half of this suite: not just its coverage
  *     (which falls back toward the ~59% this file had before), but every assertion protecting
  *     `toEntryImpl`. Measured: with the port held, mutating `size` to `0L` in the entry leaves
  *     `succeeded 14, failed 0, canceled 4` and sbt printing "All tests passed" at exit 0. The
  *     assertions in the SQL-shape tests are unaffected, because they render rather than fetch and
  *     never call lakeFS — that is the half that stays armed everywhere (verified: with the port
  *     held, breaking a projection or a where-clause connective still fails the build).
  *   - **The `null`-return test needs no stub at all.** Connection-refused, 404 and 501 all surface
  *     as `io.lakefs.clients.sdk.ApiException` (`ApiClient` wraps `IOException` in one, and non-2xx
  *     throws one directly), so that test behaves identically whether the stub bound or not.
  *
  * The stub is ~50 lines duplicated in spirit from
  * `common/workflow-core/.../LakeFSStorageClientSpec`. It cannot be shared: `build.sbt` gives
  * `WorkflowExecutionService` a test->test dependency on `DAO` and `Auth` only, so workflow-core's
  * test tree is not on this module's test classpath.
  *
  * The keyword tests RENDER a full-text predicate (they do not fetch one), which does read the
  * JVM-global `FulltextSearchQueryUtils.usePgroonga` and emits whichever arm it currently selects:
  * `pgroonga_condition(...)` when this suite runs alone, the `to_tsvector`/`to_tsquery` arm if
  * `DatasetResourceSpec` or `WorkflowResourceSpec` ran earlier in this JVM and left the global
  * `false` (both set it and neither restores it; amber has no `Test / fork`). This suite therefore
  * neither touches nor restores that global, and every keyword assertion here is deliberately
  * branch-independent: the tokens themselves and the `coalesce(...) || ' ' || coalesce(...)`
  * expression are built at `FulltextSearchQueryUtils:49-51`, *before* the `if (usePgroonga)`.
  * Anything added here must keep that property — an assertion on `pgroonga_condition` would pass
  * solo and fail in a full-module run.
  *
  * Two lines are executed but unobservable, on purpose. `val owner = record.into(USER)...` and
  * `owner.getEmail` run on every entry, yet replacing them with a bare `new User` changes nothing:
  * the dataset schema leaves `UnifiedResourceSchema`'s `userEmail` at its `DSL.inline("")` default,
  * so the translated record carries no `USER` column and `DashboardDataset.ownerEmail` is ALWAYS
  * null for dataset search results — which also makes the `leftJoin(USER)` a join that is selected
  * from and never read. That is a production defect, reported separately; asserting the null here
  * would cement it, so these tests pin the join's shape and leave the value alone.
  *
  * Not covered, and not coverable from a test:
  *   - `constructFromClause`'s `includePublic: Boolean = false` default. `scalac` emits
  *     `constructFromClause$default$3`, but `constructQuery` always passes all three arguments, so
  *     it has zero call sites and the method is `protected`.
  *   - the `LazyLogging` `logger` lazy-val bitmap on the `object` line, and scala-logging's
  *     `if (underlying.isErrorEnabled)` guard around `logger.error`. Only the enabled arm ever runs.
  *   - three of the six branch arms of `dataset.getOwnerUid == uid` (JaCoCo reports cb=3, mb=3).
  *     They are the null-safe boxed-`Integer` equality's null checks, and they describe a null
  *     `owner_uid` — a state `INT NOT NULL` plus an FK to `"user"(uid)` forbids.
  *   - `class DatasetSearchQueryBuilder {}` at the bottom of the file: a vestigial empty class with
  *     zero references repo-wide. It should be deleted, not instantiated by a test.
  *
  * Covered but unconstrainable, so no reviewer should count it as pinned behaviour:
  *   - the `.filter(_.nonEmpty)` after the keyword split. `getFullTextSearchFilter` re-applies
  *     `keywords.filter(_.nonEmpty)` itself (`FulltextSearchQueryUtils:43`), so dropping it here is
  *     an equivalent mutation — no observable differs.
  */
class DatasetSearchQueryBuilderSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val uid: Integer = Integer.valueOf(42)

  // Owner and a second signed-in caller. Both are outside java.lang.Integer's cache (-128..127) so
  // that `dataset.getOwnerUid == uid` cannot pass by reference identity.
  private val ownerUid: Integer = Integer.valueOf(9101)
  private val otherUid: Integer = Integer.valueOf(9102)

  private val sizedDid: Integer = Integer.valueOf(9001)
  private val goneDid: Integer = Integer.valueOf(9002)

  private val sizedRepo = "texera-ds-sized"
  private val goneRepo = "texera-ds-gone"

  // ---------------------------------------------------------------------------------------------
  // lakeFS loopback stub, bound at the endpoint the config already names (see the class comment).
  // ---------------------------------------------------------------------------------------------

  private val lakefsUri = new URI(StorageConfig.lakefsEndpoint)

  // Normalised, because the handler paths below are matched by exact equality and
  // StorageConfig.lakefsEndpoint is a `var` that tests are expected to override. A configured
  // ".../api/v1/" would otherwise yield "/api/v1//repositories/..." here, match nothing, and make
  // the requireStub()-guarded tests FAIL on a 404 rather than cancel -- the one failure mode the
  // class comment promises they will not have.
  private val apiPrefix = lakefsUri.getPath.stripSuffix("/")

  private val commitId = "c0ffee"

  private val commitListJson =
    s"""{"pagination":{"has_more":false,"next_offset":"","results":1,"max_per_page":1000},
       |"results":[{"id":"$commitId","parents":[],"committer":"tester","message":"m",
       |"creation_date":1700000000,"meta_range_id":"mr"}]}""".stripMargin

  private def objectStats(path: String, sizeBytes: Long): String =
    s"""{"path":"$path","path_type":"object","physical_address":"s3://bucket/$path",
       |"checksum":"chk","mtime":1700000000,"size_bytes":$sizeBytes}""".stripMargin

  /** Two objects, 10 + 32 bytes, so the size the entry carries is a value nothing else produces. */
  private val objectPageJson = {
    val objects = Seq(objectStats("small.csv", 10L), objectStats("large.csv", 32L)).mkString(",")
    s"""{"pagination":{"has_more":false,"next_offset":"","results":2,"max_per_page":1000},
       |"results":[$objects]}""".stripMargin
  }

  private var server: HttpServer = _
  private var serverPool: ExecutorService = _
  private var stubBound: Boolean = false

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length.toLong)
    exchange.getResponseBody.write(bytes)
  }

  private def handle(exchange: HttpExchange): Unit =
    try {
      exchange.getRequestBody.readAllBytes()
      exchange.getRequestURI.getPath match {
        case p if p == s"$apiPrefix/repositories/$sizedRepo/refs/main/commits" =>
          respond(exchange, 200, commitListJson)
        case p if p == s"$apiPrefix/repositories/$sizedRepo/refs/$commitId/objects/ls" =>
          respond(exchange, 200, objectPageJson)
        case _ =>
          // Everything else — including `goneRepo` — is a repository lakeFS does not have.
          respond(exchange, 404, """{"message":"repository not found"}""")
      }
    } finally exchange.close()

  /**
    * Tests that need `retrieveRepositorySize` to *succeed* can only run if the stub owns the
    * configured port. Cancelling is the documented environmental skip; it is not a failure.
    */
  private def requireStub(): Unit =
    if (!stubBound) {
      cancel(
        s"another process holds ${lakefsUri.getHost}:${lakefsUri.getPort}, " +
          s"the endpoint StorageConfig.lakefsEndpoint names; cannot serve a lakeFS size here"
      )
    }

  // ---------------------------------------------------------------------------------------------
  // Fixture
  // ---------------------------------------------------------------------------------------------

  private def user(u: Integer, name: String): User = {
    val user = new User
    user.setUid(u)
    user.setName(name)
    user.setEmail(s"$name@mail.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private def dataset(
      did: Integer,
      name: String,
      repositoryName: String,
      isDownloadable: Boolean
  ): Dataset = {
    val d = new Dataset
    d.setDid(did)
    d.setOwnerUid(ownerUid)
    d.setName(name)
    d.setRepositoryName(repositoryName)
    d.setIsPublic(true)
    d.setIsDownloadable(java.lang.Boolean.valueOf(isDownloadable))
    d.setDescription(s"description of $name")
    d.setCoverImage(s"$name.png")
    d
  }

  private def grant(did: Integer, u: Integer, privilege: PrivilegeEnum): DatasetUserAccess = {
    val access = new DatasetUserAccess
    access.setDid(did)
    access.setUid(u)
    access.setPrivilege(privilege)
    access
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()

    val cfg = getDSLContext.configuration()
    new UserDao(cfg)
      .insert(user(ownerUid, "dataset_search_owner"), user(otherUid, "dataset_search_other"))
    new DatasetDao(cfg).insert(
      dataset(sizedDid, "sized_dataset", sizedRepo, isDownloadable = false),
      dataset(goneDid, "gone_dataset", goneRepo, isDownloadable = true)
    )
    // Only the owner holds an explicit grant; `otherUid` sees the dataset purely because it is
    // public, which is what drives the PrivilegeEnum.NONE fallback.
    new DatasetUserAccessDao(cfg).insert(grant(sizedDid, ownerUid, PrivilegeEnum.WRITE))

    try {
      server = HttpServer.create(new InetSocketAddress(lakefsUri.getHost, lakefsUri.getPort), 0)
      server.createContext("/", (exchange: HttpExchange) => handle(exchange))
      serverPool = Executors.newFixedThreadPool(2)
      server.setExecutor(serverPool)
      server.start()
      stubBound = true
    } catch {
      case _: IOException => stubBound = false
    }
  }

  override protected def afterAll(): Unit = {
    try {
      if (server != null) server.stop(0)
      if (serverPool != null) serverPool.shutdownNow()
    } finally shutdownDB()
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

  /** One row of the builder's own query, narrowed to a single dataset id. */
  private def rowFor(callerUid: Integer, did: Integer): Record = {
    val p = SearchQueryParams(datasetIds = List(did).asJava)
    val rows = DatasetSearchQueryBuilder.constructQuery(callerUid, p, includePublic = true).fetch()
    rows.size() shouldBe 1
    rows.get(0)
  }

  private def entryFor(callerUid: Integer, did: Integer): DashboardClickableFileEntry =
    DatasetSearchQueryBuilder.toEntry(callerUid, rowFor(callerUid, did))

  // ---------------------------------------------------------------------------------------------
  // constructQuery: access control
  // ---------------------------------------------------------------------------------------------

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

  "the keyword filter" should "split on every character the full-text engine reserves" in {
    // A user pasting `a+b` means two terms, not a literal, and the split set is this file's own —
    // not shared with the other builders. Every character in it needs its own keyword: whatever
    // survives the split is interpolated straight into a plain-SQL condition string
    // (FulltextSearchQueryUtils:56), so a character dropped from the class reaches the rendered
    // predicate intact. `"` is the one that matters most, being a quote inside a raw SQL literal.
    val reserved = Seq('+', '-', '(', ')', '<', '>', '~', '*', '@', '"')

    reserved.zipWithIndex.foreach {
      case (reservedChar, i) =>
        val left = s"lhs$i"
        val right = s"rhs$i"
        val keyword = s"$left$reservedChar$right"
        // Rendered WITHOUT `sqlFor`'s quote-stripping: for `"` that helper would erase the very
        // character under test. Both arms of `usePgroonga` carry the surviving tokens verbatim.
        val sql = getDSLContext
          .renderInlined(
            DatasetSearchQueryBuilder.constructQuery(uid, params(keyword), includePublic = true)
          )
          .toLowerCase

        withClue(s"reserved character '$reservedChar' -> ") {
          sql should include(left)
          sql should include(right)
          sql should not include keyword
        }
    }
  }

  it should "search the dataset's name and its description, not just one of them" in {
    // The keyword tokens land in the predicate's search argument whichever columns are searched, so
    // only this concatenated expression can see the field list shrink to one column — dataset search
    // silently stopping to match descriptions, or names. The expression is built at
    // FulltextSearchQueryUtils:49-51, before the `if (usePgroonga)`, so both arms render it.
    val sql = sqlFor(uid, includePublic = true, params("alpha"))

    sql should include("coalesce(texera_db.dataset.name, '')")
    sql should include("coalesce(texera_db.dataset.description, '')")
  }

  "the where clause" should "AND its date, id and keyword filters together" in {
    // The only test that renders more than one non-empty filter at once, and therefore the only one
    // that can see the connectives: jOOQ drops `noCondition()` out of an AND chain AND out of an OR
    // chain, so with a single filter present `.and` and `.or` render identically. Swapping either
    // link turns a dashboard search into a UNION of the filters and hands back rows the caller's
    // filter excluded.
    val p = SearchQueryParams(
      keywords = List("alpha").asJava,
      datasetIds = List(sizedDid).asJava,
      creationStartDate = "2020-01-01",
      creationEndDate = "2020-12-31"
    )
    val sql = sqlFor(uid, includePublic = true, p)

    // The create-date range, on the create-date column, with the bounds in that order. Nothing else
    // in the suite passes a date, so without this the whole `getDateFilter` call is inert: it is
    // satisfied by the modified-date params, or by the two arguments swapped.
    sql should include(
      "texera_db.dataset.creation_time between timestamp '2020-01-01 00:00:00.0' " +
        "and timestamp '2020-12-31 23:59:59.999'"
    )
    // ... AND the dataset-id filter ...
    sql should include(s"and texera_db.dataset.did = $sizedDid")
    // ... AND the full-text filter. Matching only the opening paren keeps this independent of which
    // `usePgroonga` arm rendered the predicate itself.
    sql should include(s"texera_db.dataset.did = $sizedDid and (")
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

  it should "project every dataset column under the alias its schema slot names" in {
    // `toEntry` cannot see a mistake here at all. `UnifiedResourceSchema.translateRecord` writes
    // every alias back to the column the schema paired it with, so swapping ANY two same-typed
    // assignments — name/description, is_public/is_downloadable, did/ownerId — cancels out by the
    // time `toEntryImpl` reads the record and yields a byte-identical entry. The rendered SELECT is
    // the only place a permutation is visible.
    //
    // A permutation is not harmless downstream. `DashboardResource.getOrderFields` sorts on
    // `resourceName` (its "Name" case, DashboardResource:168) and on `resourceCreationTime` (its
    // "CreateTime" case), both by alias, so a swap silently re-points dashboard sorting. It is NOT
    // the frontend that reads these aliases: `searchAllResources` consumes each record into a
    // `DashboardClickableFileEntry` and returns those, so no alias survives into the HTTP response.
    // `resourceDescription` in fact has no production reader at all today —
    // `UnifiedResourceSchema.resourceDescriptionField` is referenced nowhere and `getColumnField`
    // has no "Description" case — which is worth an issue of its own; it is pinned here anyway
    // because the schema slot is what a copy-paste between builders gets wrong.
    val sql = sqlFor(uid, includePublic = true)

    sql should include("dataset.name as resourcename")
    sql should include("dataset.description as resourcedescription")
    sql should include("dataset.creation_time as resourcecreationtime")
    sql should include("dataset.owner_uid as resourceownerid")
    sql should include("dataset.did as did")
    sql should include("dataset.repository_name as repository_name")
    sql should include("dataset.is_public as is_dataset_public")
    sql should include("dataset.is_downloadable as is_dataset_downloadable")
    sql should include("dataset_user_access.privilege as user_dataset_access")
    sql should include("dataset.cover_image as cover_image")
  }

  it should "join the owner row on the dataset's owner" in {
    // `toEntryImpl` reads `owner.getEmail` out of this join, so the predicate is load-bearing on
    // paper; in practice the value is always null (see the class comment) and asserting it would
    // cement that bug. The join's shape is safe to pin and is otherwise unconstrained: nothing else
    // in the suite can tell `USER.UID.eq(DATASET.OWNER_UID)` from any other predicate, or from the
    // join being absent altogether.
    val sql = sqlFor(uid, includePublic = true)

    sql should include("left outer join texera_db.user on texera_db.user.uid = ")
    sql should include("texera_db.user.uid = texera_db.dataset.owner_uid")
  }

  // ---------------------------------------------------------------------------------------------
  // toEntry
  // ---------------------------------------------------------------------------------------------

  "toEntry" should "carry every dataset column and the lakeFS repository size into the entry" in {
    requireStub()

    val entry = entryFor(ownerUid, sizedDid)
    entry should not be null

    val dd = entry.dataset.value
    dd.dataset.getDid shouldBe sizedDid
    dd.dataset.getOwnerUid shouldBe ownerUid
    dd.dataset.getName shouldBe "sized_dataset"
    dd.dataset.getDescription shouldBe "description of sized_dataset"
    dd.dataset.getRepositoryName shouldBe sizedRepo
    dd.dataset.getIsPublic shouldBe true
    // The fixture flips this away from the column default, so a slot wired to a literal (rather
    // than to a DATASET column) reads back as null here rather than accidentally matching. What it
    // cannot see is a swap with `is_public`: see the projection test for why, and for where that is
    // pinned instead.
    dd.dataset.getIsDownloadable shouldBe false
    dd.dataset.getCoverImage shouldBe "sized_dataset.png"
    // DDL-defaulted, so this is null only if the schema stops projecting the column at all.
    dd.dataset.getCreationTime should not be null
    // 10 + 32 across the newest commit's two objects. This is the number the dashboard shows.
    dd.size shouldBe 42L
  }

  it should "set isOwner only for the dataset's own owner" in {
    requireStub()

    entryFor(ownerUid, sizedDid).dataset.value.isOwner shouldBe true
    entryFor(otherUid, sizedDid).dataset.value.isOwner shouldBe false
    // An anonymous caller reaches the dataset because it is public, and owns nothing.
    entryFor(null, sizedDid).dataset.value.isOwner shouldBe false
  }

  it should "report the granted privilege, and fall back to NONE with no grant row" in {
    requireStub()

    entryFor(ownerUid, sizedDid).dataset.value.accessPrivilege shouldBe PrivilegeEnum.WRITE
    // `otherUid` has no dataset_user_access row: the left join yields null, and null must read as
    // NONE rather than propagate to the client as a missing privilege.
    entryFor(otherUid, sizedDid).dataset.value.accessPrivilege shouldBe PrivilegeEnum.NONE
  }

  it should "tag the entry as a dataset and fill the dataset payload slot" in {
    // `entry.workflow shouldBe None` / `entry.project shouldBe None` used to sit here and were
    // vacuous: `DashboardClickableFileEntry` declares both `= None` (DashboardResource:40-41) and
    // this file passes neither, so no mutation of `toEntryImpl` can falsify them — they assert
    // another file's case-class defaults. Only `resourceType` and `dataset` are this file's to set.
    //
    // `resourceType` here has the production constant on the right-hand side, so retargeting the
    // constant's *value* moves both sides together; the inlined-literal assertion in
    // "tag its rows as datasets" is what actually pins the value. The `getDid` check below is an
    // identity check on the payload — that the entry carries the row it was built from — and is
    // redundant with the column test rather than newly discriminating.
    requireStub()

    val entry = entryFor(ownerUid, sizedDid)
    entry.resourceType shouldBe SearchQueryBuilder.DATASET_RESOURCE_TYPE
    entry.dataset should be(defined)
    entry.dataset.value.dataset.getDid shouldBe sizedDid
  }

  it should "drop the row entirely when lakeFS rejects the repository" in {
    // No `requireStub()`: a 404 from the stub, a 404 from a real lakeFS and a refused connection all
    // arrive as io.lakefs.clients.sdk.ApiException, so this holds however the port is occupied.
    //
    // The null is load-bearing. DashboardResource filters these rows out and raises `hasMismatch`
    // from them; without it a dataset whose repository is gone is shown to the user as 0 bytes.
    DatasetSearchQueryBuilder.toEntry(ownerUid, rowFor(ownerUid, goneDid)) shouldBe null
  }
}
