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

package org.apache.texera.amber.core.storage.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.common.tags.NonParallelTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.{InetAddress, InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.{ConcurrentLinkedQueue, ExecutorService, Executors}
import scala.jdk.CollectionConverters._

/**
  * Request/response records for the loopback stub below.
  *
  * Top-level (rather than nested in the suite) so that `case StubRequest(...)` patterns are not
  * path-dependent — the compiler cannot check the outer reference of an inner case class at run
  * time and warns on every match site.
  */
private object LakeFSStubServer {
  final case class StubRequest(
      method: String,
      path: String,
      query: Map[String, String],
      body: String
  )

  final case class StubResponse(status: Int, body: String = "")
}

/**
  * Spec for the parts of [[LakeFSStorageClient]] that do not need a real lakeFS server:
  *
  *   - pure helpers (`parsePhysicalAddress`, the `initRepo` name validation), and
  *   - the request/response wiring, driven against a loopback stub that speaks just enough of the
  *     lakeFS REST API for the generated SDK to be satisfied.
  *
  * The stub is deliberately *not* a way to test the SDK's URL templates. What it pins is the set of
  * choices this class makes on top of the SDK and that nothing else can observe:
  *
  *   - which ref each call targets — several methods hard-code the `main` branch
  *     (`deleteObject`, `resetObjectUploadOrDeletion`, the multipart calls) while their siblings
  *     take a caller-supplied branch (`createCommit`) or a commit hash (`getFileSize`,
  *     `getFilePresignedUrl`, `getFileFromRepo`); mixing them up is silent and destructive;
  *   - the `fetchAllPages` loop — cursor threading and page accumulation, which a live server can
  *     only exercise past 1000 objects;
  *   - `retrieveVersionsOfRepository`'s newest-commit-first ordering, which a live server hides
  *     because it already returns commits newest-first;
  *   - `completePresignedMultipartUploads`'s part sort, which a live server absorbs silently;
  *   - which field of a stat response each getter returns.
  *
  * A stub is not just a convenience here: only against one can you assert that ZERO bytes left the
  * process (see the `initRepo` validation test). A live server can distinguish reject-before-send
  * from server-side reject by exception type, but not prove nothing was sent.
  *
  * Tagged [[NonParallelTest]] so `common/workflow-core/build.sbt` gives this suite its own forked
  * JVM. That is load-bearing, not cosmetic: `LakeFSStorageClient.apiClient` is a `lazy val` that
  * captures `StorageConfig.lakefsEndpoint` once per JVM, and `LakeFSStorageClientMtimeSpec` points
  * that same endpoint at a testcontainer. The two suites must never share a JVM; both are tagged,
  * so isolation survives either tag being dropped.
  */
@NonParallelTest
class LakeFSStorageClientSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  // ---------------------------------------------------------------------------------------------
  // Loopback stub
  // ---------------------------------------------------------------------------------------------

  import LakeFSStubServer._

  private val requests = new ConcurrentLinkedQueue[StubRequest]()

  private val notStubbed: StubRequest => StubResponse =
    req => StubResponse(501, s"""{"message":"no stub route for ${req.method} ${req.path}"}""")

  @volatile private var route: StubRequest => StubResponse = notStubbed

  private var server: HttpServer = _
  private var serverPool: ExecutorService = _

  private def decode(s: String): String = URLDecoder.decode(s, UTF_8.name())

  private def handle(exchange: HttpExchange): Unit = {
    try {
      val body = new String(exchange.getRequestBody.readAllBytes(), UTF_8)
      val query = Option(exchange.getRequestURI.getRawQuery)
        .filter(_.nonEmpty)
        .map(_.split("&").toList.map { pair =>
          pair.indexOf('=') match {
            case -1 => decode(pair) -> ""
            case i  => decode(pair.substring(0, i)) -> decode(pair.substring(i + 1))
          }
        }.toMap)
        .getOrElse(Map.empty[String, String])

      val request =
        StubRequest(exchange.getRequestMethod, exchange.getRequestURI.getPath, query, body)
      requests.add(request)

      val response =
        try route(request)
        catch { case t: Throwable => StubResponse(500, s"""{"message":"${t.getClass.getName}"}""") }

      val bytes = response.body.getBytes(UTF_8)
      if (bytes.isEmpty) {
        // -1 means "no response body"; the SDK maps 204 to a null (Unit) return.
        exchange.sendResponseHeaders(response.status, -1L)
      } else {
        exchange.getResponseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(response.status, bytes.length.toLong)
        exchange.getResponseBody.write(bytes)
      }
    } finally exchange.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), 0)
    server.createContext("/", (exchange: HttpExchange) => handle(exchange))
    serverPool = Executors.newFixedThreadPool(2)
    server.setExecutor(serverPool)
    server.start()
    // Must happen before anything forces LakeFSStorageClient.apiClient (a JVM-wide lazy val).
    // Nothing above this line touches the client, and no other suite shares this forked JVM.
    StorageConfig.lakefsEndpoint = s"http://127.0.0.1:${server.getAddress.getPort}/api/v1"
  }

  override def afterAll(): Unit = {
    try {
      if (server != null) server.stop(0)
      if (serverPool != null) serverPool.shutdownNow()
    } finally super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    requests.clear()
    route = notStubbed
  }

  /** Installs the routes for one test; anything unmatched answers 501 and shows up in `recorded`. */
  private def stub(routes: PartialFunction[StubRequest, StubResponse]): Unit =
    route = req => routes.applyOrElse(req, notStubbed)

  private def recorded: List[StubRequest] = requests.asScala.toList

  /**
    * True only for the FIRST request this test makes to `path`. Page-1 routes are guarded with it so
    * that a broken pagination cursor produces a clean 501 -> ApiException instead of an infinite
    * loop: without it, a client that stops threading `after` re-matches the `has_more:true` page-1
    * route forever and the suite hangs rather than failing. `requests.add` runs before the route is
    * applied, so the current request is already counted.
    */
  private def firstHitOf(path: String): Boolean = recorded.count(_.path == path) == 1

  private def onlyRequest: StubRequest = {
    recorded should have size 1
    recorded.head
  }

  private val mapper = new ObjectMapper()
  private def json(body: String) = mapper.readTree(body)

  // ---------------------------------------------------------------------------------------------
  // Response fixtures. Only the fields the SDK marks required, plus whatever a test reads back.
  // ---------------------------------------------------------------------------------------------

  private def objectStats(path: String, sizeBytes: Long, physicalAddress: String): String =
    s"""{"path":"$path","path_type":"object","physical_address":"$physicalAddress",
       |"checksum":"chk","mtime":1700000000,"size_bytes":$sizeBytes}""".stripMargin

  private def pagination(hasMore: Boolean, nextOffset: String, results: Int): String =
    s"""{"has_more":$hasMore,"next_offset":"$nextOffset","results":$results,"max_per_page":1000}"""

  private def objectPage(
      hasMore: Boolean,
      nextOffset: String,
      objects: (String, Long)*
  ): String = {
    val results = objects.map { case (p, s) => objectStats(p, s, s"s3://bucket/$p") }.mkString(",")
    s"""{"pagination":${pagination(hasMore, nextOffset, objects.size)},"results":[$results]}"""
  }

  private def diffPage(hasMore: Boolean, nextOffset: String, paths: String*): String = {
    val results = paths.map(p => s"""{"type":"added","path":"$p","path_type":"object"}""")
    s"""{"pagination":${pagination(hasMore, nextOffset, paths.size)},
       |"results":[${results.mkString(",")}]}""".stripMargin
  }

  private def commitList(commits: (String, Long)*): String = {
    val results = commits
      .map {
        case (id, createdAt) =>
          s"""{"id":"$id","parents":[],"committer":"tester","message":"m",
             |"creation_date":$createdAt,"meta_range_id":"mr"}""".stripMargin
      }
      .mkString(",")
    s"""{"pagination":${pagination(false, "", commits.size)},"results":[$results]}"""
  }

  private def repository(id: String): String =
    s"""{"id":"$id","creation_date":1700000000,"default_branch":"main",
       |"storage_namespace":"s3://bucket/$id"}""".stripMargin

  private def commit(id: String): String =
    s"""{"id":"$id","parents":[],"committer":"tester","message":"m",
       |"creation_date":1700000000,"meta_range_id":"mr"}""".stripMargin

  // ---------------------------------------------------------------------------------------------
  // parsePhysicalAddress
  // ---------------------------------------------------------------------------------------------

  // `healthCheck` retries through the shared `RetryUtil.withBackoff`; that contract (progression,
  // give-up wrapping, interrupt fail-fast) is covered by `RetryUtilSpec` in `common/util`.

  "parsePhysicalAddress" should "split a well-formed address into bucket and key" in {
    assert(
      LakeFSStorageClient.parsePhysicalAddress("s3://my-bucket/path/to/file.csv") ==
        (("my-bucket", "path/to/file.csv"))
    )
    // key should have its leading slash stripped and preserve nested segments
    assert(
      LakeFSStorageClient.parsePhysicalAddress("gs://another-bucket/some/prefix/data.json") ==
        (("another-bucket", "some/prefix/data.json"))
    )
  }

  it should "throw for an empty or blank address" in {
    val emptyEx = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("")
    }
    assert(emptyEx.getMessage.contains("empty"))

    val blankEx = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("   ")
    }
    assert(blankEx.getMessage.contains("empty"))
  }

  it should "throw when the address is not a valid URI" in {
    val ex = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("s3://bad host/key")
    }
    assert(ex.getMessage.contains("Invalid address URI"))
    assert(ex.getCause != null)
  }

  it should "throw when the address is missing a host/bucket" in {
    val ex = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("s3:///only-a-key")
    }
    assert(ex.getMessage.contains("missing host/bucket"))
  }

  it should "throw when the address is missing a key/path" in {
    val noPathEx = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("s3://my-bucket")
    }
    assert(noPathEx.getMessage.contains("missing key/path"))

    // a trailing slash yields an empty key after stripping, which is also invalid
    val rootPathEx = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("s3://my-bucket/")
    }
    assert(rootPathEx.getMessage.contains("missing key/path"))
  }

  it should "reject a null address instead of dereferencing it" in {
    // The `Option(address)` guard is what turns a null into an IllegalArgumentException instead
    // of an NPE. (Its one caller already null-guards upstream, so this pins the documented
    // @throws contract rather than a reachable production path.)
    val ex = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress(null)
    }
    assert(ex.getMessage.contains("empty"))
  }

  it should "trim surrounding whitespace before parsing" in {
    // Without the leading trim the URI parse fails (a space is not a legal URI character), so this
    // pins the trim rather than restating the happy path.
    assert(
      LakeFSStorageClient.parsePhysicalAddress("  s3://my-bucket/path/to/file.csv \n") ==
        (("my-bucket", "path/to/file.csv"))
    )
  }

  // ---------------------------------------------------------------------------------------------
  // initRepo
  // ---------------------------------------------------------------------------------------------

  "initRepo" should "reject malformed repository names without issuing a request" in {
    val invalid = Seq(
      "ab" -> "two characters is below the 3-character minimum",
      "a" * 64 -> "64 characters is above the 63-character maximum",
      "-abc" -> "a leading hyphen is not allowed",
      "Abc" -> "uppercase is not allowed",
      "a_bc" -> "underscores are not allowed",
      "a.bc" -> "dots are not allowed",
      "" -> "empty is not a name"
    )

    invalid.foreach {
      case (name, why) =>
        val ex = intercept[IllegalArgumentException] {
          LakeFSStorageClient.initRepo(name)
        }
        withClue(s"$name ($why): ") {
          ex.getMessage should include(s"'$name'")
        }
    }

    // Validation is client-side: a rejected name must never reach lakeFS. Only a stub can show
    // this — against a live server a rejected create and a never-sent create look identical.
    recorded shouldBe empty
  }

  it should "accept the shortest and longest legal repository names" in {
    // Pins both ends of the {2,62}-after-first-character window: 3 and 63 characters inclusive.
    // Without this, narrowing the quantifier by one on either side goes unnoticed.
    Seq("abc", "a" * 63).foreach { name =>
      requests.clear()
      stub {
        case StubRequest("POST", "/api/v1/repositories", _, _) =>
          StubResponse(201, repository(name))
      }
      LakeFSStorageClient.initRepo(name).getId shouldEqual name
      json(onlyRequest.body).get("name").asText() shouldEqual name
    }
  }

  it should "create the repository under a per-repository storage namespace on the main branch" in {
    val name = "texera-init-repo"
    stub {
      case StubRequest("POST", "/api/v1/repositories", _, _) => StubResponse(201, repository(name))
    }

    LakeFSStorageClient.initRepo(name)

    val body = json(onlyRequest.body)
    body.get("name").asText() shouldEqual name
    body.get("default_branch").asText() shouldEqual "main"
    body.get("sample_data").asBoolean() shouldEqual false
    // Every repository gets its own prefix *inside* the shared bucket — two repositories must not
    // land on the same namespace, which is what the trailing "/<repo>" segment guarantees.
    val namespace = body.get("storage_namespace").asText()
    namespace should startWith(
      s"${StorageConfig.lakefsBlockStorageType}://${StorageConfig.lakefsBucketName}"
    )
    namespace should endWith(s"/$name")
  }

  // ---------------------------------------------------------------------------------------------
  // Pagination (fetchAllPages)
  // ---------------------------------------------------------------------------------------------

  "retrieveObjectsOfVersion" should "follow the pagination cursor and concatenate every page" in {
    val repo = "texera-paging"
    val hash = "commit-abc"
    stub {
      case StubRequest("GET", p, q, _)
          if p == s"/api/v1/repositories/$repo/refs/$hash/objects/ls" && !q.contains(
            "after"
          ) && firstHitOf(p) =>
        StubResponse(200, objectPage(hasMore = true, "cursor-1", "a.txt" -> 1L, "b.txt" -> 2L))
      case StubRequest("GET", p, q, _)
          if p == s"/api/v1/repositories/$repo/refs/$hash/objects/ls" &&
            q.get("after").contains("cursor-1") =>
        StubResponse(200, objectPage(hasMore = false, "", "c.txt" -> 3L))
    }

    val objects = LakeFSStorageClient.retrieveObjectsOfVersion(repo, hash)

    // Page 2 is appended to page 1, in page order — not overwritten by the last page.
    objects.map(_.getPath) shouldEqual List("a.txt", "b.txt", "c.txt")

    recorded should have size 2
    // The first page is requested with no cursor; the second carries the cursor the first returned.
    recorded.head.query.get("after") shouldBe None
    recorded(1).query.get("after") shouldBe Some("cursor-1")
    // Both pages ask for the class's page size, so a 110-object repository is one round trip.
    recorded.map(_.query.get("amount")) shouldEqual List(Some("1000"), Some("1000"))
  }

  "retrieveUncommittedObjects" should "page through the diff of the main branch" in {
    val repo = "texera-uncommitted"
    stub {
      case StubRequest("GET", p, q, _)
          if p == s"/api/v1/repositories/$repo/branches/main/diff" && !q.contains(
            "after"
          ) && firstHitOf(p) =>
        StubResponse(200, diffPage(hasMore = true, "cursor-1", "staged/one.bin"))
      case StubRequest("GET", p, q, _)
          if p == s"/api/v1/repositories/$repo/branches/main/diff" &&
            q.get("after").contains("cursor-1") =>
        StubResponse(200, diffPage(hasMore = false, "", "staged/two.bin"))
    }

    LakeFSStorageClient
      .retrieveUncommittedObjects(repo)
      .map(_.getPath) shouldEqual List("staged/one.bin", "staged/two.bin")

    // Uncommitted work only ever lives on the write branch, so the diff is pinned to main.
    recorded.map(_.path).distinct shouldEqual List(s"/api/v1/repositories/$repo/branches/main/diff")
    recorded should have size 2
  }

  // ---------------------------------------------------------------------------------------------
  // retrieveRepositorySize
  // ---------------------------------------------------------------------------------------------

  "retrieveRepositorySize" should "sum every page of a given commit without reading the log" in {
    val repo = "texera-size"
    val hash = "commit-xyz"
    stub {
      case StubRequest("GET", p, q, _)
          if p == s"/api/v1/repositories/$repo/refs/$hash/objects/ls" && !q.contains(
            "after"
          ) && firstHitOf(p) =>
        StubResponse(200, objectPage(hasMore = true, "cursor-1", "a" -> 10L, "b" -> 20L))
      case StubRequest("GET", p, q, _)
          if p == s"/api/v1/repositories/$repo/refs/$hash/objects/ls" &&
            q.get("after").contains("cursor-1") =>
        StubResponse(200, objectPage(hasMore = false, "", "c" -> 5L))
    }

    LakeFSStorageClient.retrieveRepositorySize(repo, hash) shouldEqual 35L
    // An explicit hash short-circuits the commit lookup entirely.
    recorded.exists(_.path.endsWith("/commits")) shouldBe false
  }

  it should "measure the newest commit when no commit hash is given" in {
    val repo = "texera-size-head"
    // Deliberately out of order, and with the newest neither first nor last in the response, so
    // neither `.head` nor `.last` of the raw list would pick it.
    stub {
      case StubRequest("GET", p, _, _) if p == s"/api/v1/repositories/$repo/refs/main/commits" =>
        StubResponse(200, commitList("older" -> 100L, "newest" -> 300L, "middle" -> 200L))
      case StubRequest("GET", p, _, _)
          if p == s"/api/v1/repositories/$repo/refs/newest/objects/ls" =>
        StubResponse(200, objectPage(hasMore = false, "", "a" -> 7L, "b" -> 8L))
    }

    LakeFSStorageClient.retrieveRepositorySize(repo) shouldEqual 15L
    // The objects listed are the ones at the newest commit, not at whichever came back first.
    recorded.map(_.path) shouldEqual List(
      s"/api/v1/repositories/$repo/refs/main/commits",
      s"/api/v1/repositories/$repo/refs/newest/objects/ls"
    )
  }

  it should "report zero for a repository with no commits, without listing objects" in {
    val repo = "texera-size-empty"
    stub {
      case StubRequest("GET", p, _, _) if p == s"/api/v1/repositories/$repo/refs/main/commits" =>
        StubResponse(200, commitList())
    }

    LakeFSStorageClient.retrieveRepositorySize(repo) shouldEqual 0L
    // Listing at the empty ref would be a request lakeFS rejects; the guard must stop first.
    onlyRequest.path shouldEqual s"/api/v1/repositories/$repo/refs/main/commits"
  }

  // ---------------------------------------------------------------------------------------------
  // Presigned multipart uploads
  // ---------------------------------------------------------------------------------------------

  "initiatePresignedMultipartUploads" should "request the given part count on the main branch" in {
    val repo = "texera-mpu"
    val path = "data/big.bin"
    stub {
      case StubRequest("POST", p, _, _)
          if p == s"/api/v1/repositories/$repo/branches/main/staging/pmpu" =>
        StubResponse(201, """{"upload_id":"upload-1","physical_address":"s3://bucket/phys"}""")
    }

    val upload = LakeFSStorageClient.initiatePresignedMultipartUploads(repo, path, 7)
    upload.getUploadId shouldEqual "upload-1"
    upload.getPhysicalAddress shouldEqual "s3://bucket/phys"

    onlyRequest.query.get("parts") shouldBe Some("7")
    onlyRequest.query.get("path") shouldBe Some(path)
  }

  "completePresignedMultipartUploads" should "send parts ordered by part number" in {
    val repo = "texera-mpu"
    val path = "data/big.bin"
    stub {
      case StubRequest("PUT", p, _, _)
          if p == s"/api/v1/repositories/$repo/branches/main/staging/pmpu/upload-1" =>
        StubResponse(200, objectStats(path, 3L, "s3://bucket/phys"))
    }

    val stats = LakeFSStorageClient.completePresignedMultipartUploads(
      repo,
      path,
      "upload-1",
      // Callers collect part ETags concurrently, so they arrive shuffled; S3 rejects a
      // completion whose parts are not in ascending part-number order.
      List(3 -> "etag-3", 1 -> "etag-1", 2 -> "etag-2"),
      "s3://bucket/phys"
    )
    stats.getPath shouldEqual path

    val body = json(onlyRequest.body)
    val parts = body.get("parts").elements().asScala.toList
    parts.map(_.get("part_number").asInt()) shouldEqual List(1, 2, 3)
    // ETags must ride along with their own part number, not just be sorted independently.
    parts.map(_.get("etag").asText()) shouldEqual List("etag-1", "etag-2", "etag-3")
    body.get("physical_address").asText() shouldEqual "s3://bucket/phys"
    onlyRequest.query.get("path") shouldBe Some(path)
  }

  "abortPresignedMultipartUploads" should "delete the upload and name its physical address" in {
    val repo = "texera-mpu"
    val path = "data/big.bin"
    stub {
      case StubRequest("DELETE", p, _, _)
          if p == s"/api/v1/repositories/$repo/branches/main/staging/pmpu/upload-1" =>
        StubResponse(204)
    }

    LakeFSStorageClient.abortPresignedMultipartUploads(repo, path, "upload-1", "s3://bucket/phys")

    // lakeFS needs the physical address to clean up the backing object; omitting it leaks the
    // multipart parts in the bucket.
    json(onlyRequest.body).get("physical_address").asText() shouldEqual "s3://bucket/phys"
    onlyRequest.query.get("path") shouldBe Some(path)
  }

  // ---------------------------------------------------------------------------------------------
  // Branch and object mutations
  // ---------------------------------------------------------------------------------------------

  "resetObjectUploadOrDeletion" should "reset exactly one object path on the main branch" in {
    val repo = "texera-reset"
    stub {
      case StubRequest("PUT", p, _, _) if p == s"/api/v1/repositories/$repo/branches/main" =>
        StubResponse(204)
    }

    LakeFSStorageClient.resetObjectUploadOrDeletion(repo, "staged/one.bin")

    val body = json(onlyRequest.body)
    // "object" scopes the reset to the single path. The other ResetCreation types ("common_prefix"
    // and "reset") would discard other users' staged work on the same branch.
    body.get("type").asText() shouldEqual "object"
    body.get("path").asText() shouldEqual "staged/one.bin"
  }

  "createCommit" should "commit on the caller-supplied branch" in {
    val repo = "texera-commit"
    stub {
      case StubRequest("POST", p, _, _)
          if p == s"/api/v1/repositories/$repo/branches/feature-x/commits" =>
        StubResponse(201, commit("commit-1"))
    }

    // Unlike the other mutating calls in this class, createCommit is branch-parameterised; falling
    // back to the hard-coded main branch would commit to the wrong place.
    LakeFSStorageClient.createCommit(repo, "feature-x", "a message").getId shouldEqual "commit-1"
    json(onlyRequest.body).get("message").asText() shouldEqual "a message"
  }

  "deleteObject" should "delete the staged path on the main branch" in {
    val repo = "texera-delete"
    stub {
      case StubRequest("DELETE", p, _, _)
          if p == s"/api/v1/repositories/$repo/branches/main/objects" =>
        StubResponse(204)
    }

    LakeFSStorageClient.deleteObject(repo, "staged/one.bin")
    onlyRequest.query.get("path") shouldBe Some("staged/one.bin")
  }

  // Left uncovered on purpose:
  //   - `deleteRepo` is a one-line passthrough with no choice of its own to assert, so a test
  //     could only restate its body;
  //   - `removeFileFromRepo` and `retrieveFileContent` have no caller anywhere in the repo, and
  //     `withCreateVersion` only has one in `file-service`'s DatasetResourceSpec — testing them
  //     here would make dead code harder to delete.

  // ---------------------------------------------------------------------------------------------
  // Read-side getters
  // ---------------------------------------------------------------------------------------------

  "getFilePresignedUrl" should "ask for a presigned stat and return the physical address" in {
    val repo = "texera-presign"
    val hash = "commit-abc"
    stub {
      case StubRequest("GET", p, _, _)
          if p == s"/api/v1/repositories/$repo/refs/$hash/objects/stat" =>
        StubResponse(200, objectStats("data/x.csv", 12L, "https://signed.example/x.csv?sig=1"))
    }

    // The presigned URL is the physical address of a *presigned* stat; without presign=true lakeFS
    // returns the raw storage address, which the browser cannot fetch.
    LakeFSStorageClient.getFilePresignedUrl(repo, hash, "data/x.csv") shouldEqual
      "https://signed.example/x.csv?sig=1"
    onlyRequest.query.get("presign") shouldBe Some("true")
    onlyRequest.query.get("path") shouldBe Some("data/x.csv")
  }

  "getFileSize" should "return the object size at the given commit" in {
    val repo = "texera-size-of"
    val hash = "commit-abc"
    stub {
      case StubRequest("GET", p, _, _)
          if p == s"/api/v1/repositories/$repo/refs/$hash/objects/stat" =>
        // mtime in the fixture is 1700000000; returning it instead of size_bytes would be a
        // plausible slip, and this value is far from it.
        StubResponse(200, objectStats("data/x.csv", 4242L, "s3://bucket/x"))
    }

    LakeFSStorageClient.getFileSize(repo, hash, "data/x.csv") shouldEqual 4242L
    onlyRequest.query.get("path") shouldBe Some("data/x.csv")
  }

  "getFileFromRepo" should "download the object at the requested version, not the main branch" in {
    val repo = "texera-get"
    val hash = "commit-abc"
    val content = "hello-lakefs"
    stub {
      case StubRequest("GET", p, _, _) if p == s"/api/v1/repositories/$repo/refs/$hash/objects" =>
        StubResponse(200, content)
    }

    val file = LakeFSStorageClient.getFileFromRepo(repo, hash, "data/x.csv")
    try {
      new String(Files.readAllBytes(file.toPath), UTF_8) shouldEqual content
      // Reading a historical version off the mutable main branch would silently return the wrong
      // bytes; the ref segment has to be the commit hash.
      onlyRequest.path shouldEqual s"/api/v1/repositories/$repo/refs/$hash/objects"
      onlyRequest.query.get("path") shouldBe Some("data/x.csv")
    } finally Files.deleteIfExists(file.toPath)
  }

  // ---------------------------------------------------------------------------------------------
  // healthCheck
  // ---------------------------------------------------------------------------------------------

  "healthCheck" should "return after a single probe when the server is healthy" in {
    stub {
      case StubRequest("GET", "/api/v1/healthcheck", _, _) => StubResponse(204)
    }

    LakeFSStorageClient.healthCheck()

    // The retry wrapper must not re-probe a server that answered; FileService calls this on
    // startup, and an unconditional retry would add seconds to every boot.
    onlyRequest.path shouldEqual "/api/v1/healthcheck"
  }
}
