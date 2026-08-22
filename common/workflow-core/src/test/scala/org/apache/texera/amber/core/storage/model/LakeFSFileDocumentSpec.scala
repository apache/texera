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

package org.apache.texera.amber.core.storage.model

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.texera.common.config.{EnvironmentalVariable, StorageConfig}
import org.apache.texera.amber.core.storage.ResourceType
import org.apache.texera.common.tags.NonParallelTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.{ByteArrayInputStream, InputStream}
import java.net.{InetSocketAddress, URI, URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ConcurrentLinkedQueue, ExecutorService, Executors}
import scala.jdk.CollectionConverters._

/**
  * Spec for [[LakeFSFileDocument]]: URI parsing, and the read path that turns the parsed
  * repository/commit/file triple into bytes.
  *
  * The read path is driven against a loopback [[HttpServer]] that speaks the three HTTP hops the
  * document can make — the lakeFS `objects/stat?presign=true` lookup, a GET of the presigned
  * address that lookup returns, and the direct lakeFS `objects` download used as a fallback. No
  * lakeFS server (and no Docker) is involved.
  *
  * Every hop is served by the same stub, and the presigned address points back at it, which is
  * what lets a test tell "served through the presigned URL" apart from "fell back to lakeFS": the
  * two routes return different bytes. Against a real lakeFS both paths return the same file, so
  * the fallback could silently become the only live path.
  *
  * These tests exercise the `userJwtToken.isEmpty` half of `asInputStream`. That is the half every
  * test JVM takes: `USER_JWT_TOKEN` is injected only into the pod env of a user-created computing
  * unit (`ComputingUnitManagingResource`), never in CI or a dev shell. The non-empty half — which
  * asks a file-service presign endpoint on a fixed port for the URL — is left to file-service's own
  * tests; reaching it here would need a hardcoded port in `build.sbt`'s test grouping.
  *
  * Tagged [[NonParallelTest]] so `common/workflow-core/build.sbt` gives this suite its own forked
  * JVM. That is load-bearing: `beforeAll` repoints `StorageConfig.lakefsEndpoint`, a JVM-wide
  * `var` that `LakeFSStorageClient.apiClient` (a `lazy val`) captures once per JVM, and
  * `LakeFSStorageClientSpec` / `LakeFSStorageClientMtimeSpec` point that same endpoint at their own
  * servers.
  */
@NonParallelTest
class LakeFSFileDocumentSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  // Realistic 40-char git commit hash, mirroring the URIs produced by FileResolver
  // (format: dataset:///{repositoryName}/{versionHash}/{fileRelativePath}).
  private val versionHash = "97fd4c2a755b69b7c66d322eab40b7e5c2ad5d10"

  // URI parsing is shared by every resource type; exercise it through the dataset type.
  private def datasetDoc(uri: URI) = new LakeFSFileDocument(uri, ResourceType.Dataset)

  // -----------------------------------------------------------------------------------------
  // Loopback stub
  // -----------------------------------------------------------------------------------------

  /** One recorded request. Fields are read directly; never destructured in a `case` pattern. */
  private case class Hit(method: String, path: String, query: Map[String, String])

  private val requests = new ConcurrentLinkedQueue[Hit]()
  private var server: HttpServer = _
  private var serverPool: ExecutorService = _

  /** Request paths that should answer 500 instead of being served, for this one test. */
  @volatile private var failing: Set[String] = Set.empty

  private val repositoryName = "texera-file-doc"
  private val fileName = "records.csv"
  // Most dataset and model files live under a directory, so the read path is driven over both
  // shapes: `fileName` at the repository root, and this nested path.
  //
  // getFileRelativePath() renders a parsed path with the *platform* separator, so every expectation
  // over a multi-segment path — here and in the URI block above — is derived through Paths.get
  // exactly as production renders it. Spelling out a literal "nested/dir/records.csv" instead would
  // assert a normalization the class does not do (it would fail on Windows today), not a choice the
  // class makes.
  private val nestedSegments = Seq("nested", "dir", fileName)

  private def statPath = s"/api/v1/repositories/$repositoryName/refs/$versionHash/objects/stat"
  private def objectPath = s"/api/v1/repositories/$repositoryName/refs/$versionHash/objects"
  private def presignedPath = "/signed-blob/records.csv"

  // Longer than asFile's 1024-byte copy buffer and not a multiple of it (2502 bytes), so the copy
  // loop runs several times and ends on a short read that must not be padded out to a full buffer.
  private val presignedContent: String = "presigned-payload;" * 139
  // Deliberately different bytes, and a different length, from the presigned payload.
  private val fallbackContent: String = "direct-lakefs-download"

  private def presignedUrl: String =
    s"http://127.0.0.1:${server.getAddress.getPort}$presignedPath"

  private def objectStatsJson: String =
    s"""{"path":"$fileName","path_type":"object","physical_address":"$presignedUrl",
       |"checksum":"chk","mtime":1700000000,"size_bytes":${presignedContent.length}}""".stripMargin

  private def handle(exchange: HttpExchange): Unit = {
    try {
      val uri = exchange.getRequestURI
      val query = Option(uri.getRawQuery)
        .filter(_.nonEmpty)
        .map(_.split("&").toList.map { pair =>
          def dec(s: String) = URLDecoder.decode(s, StandardCharsets.UTF_8.name())
          pair.indexOf('=') match {
            case -1 => dec(pair) -> ""
            case i  => dec(pair.substring(0, i)) -> dec(pair.substring(i + 1))
          }
        }.toMap)
        .getOrElse(Map.empty[String, String])
      requests.add(Hit(exchange.getRequestMethod, uri.getPath, query))

      val (status, body) =
        if (failing.contains(uri.getPath)) (500, """{"message":"stubbed failure"}""")
        else
          uri.getPath match {
            case p if p == statPath      => (200, objectStatsJson)
            case p if p == presignedPath => (200, presignedContent)
            case p if p == objectPath    => (200, fallbackContent)
            case p                       => (501, s"""{"message":"no stub route for $p"}""")
          }

      val bytes = body.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.set("Content-Type", "application/json")
      exchange.sendResponseHeaders(status, bytes.length.toLong)
      exchange.getResponseBody.write(bytes)
    } finally exchange.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", (exchange: HttpExchange) => handle(exchange))
    serverPool = Executors.newFixedThreadPool(2)
    server.setExecutor(serverPool)
    server.start()
    // Must happen before anything forces LakeFSStorageClient.apiClient (a JVM-wide lazy val).
    // Nothing above this line touches that object, and no other suite shares this forked JVM.
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
    failing = Set.empty
  }

  private def hits: List[Hit] = requests.asScala.toList

  /** A document over the file the stub serves. */
  private def servedDoc(): LakeFSFileDocument =
    datasetDoc(new URI(s"dataset:///$repositoryName/$versionHash/$fileName"))

  /** The same file two directories deep — the shape most dataset and model files have. */
  private def nestedDoc(): LakeFSFileDocument =
    datasetDoc(new URI(s"dataset:///$repositoryName/$versionHash/${nestedSegments.mkString("/")}"))

  /** How the document renders that nested path, derived the same way production renders it. */
  private def nestedRelativePath: String =
    Paths.get(nestedSegments.head, nestedSegments.tail: _*).toString

  private def readFully(doc: LakeFSFileDocument): String = {
    val stream = doc.asInputStream()
    try new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    finally stream.close()
  }

  "LakeFSFileDocument" should "parse a valid 3-segment dataset URI into its components" in {
    val uri = new URI(s"dataset:///test_dataset/$versionHash/1.txt")
    val doc = datasetDoc(uri)

    doc.getRepositoryName() shouldBe "test_dataset"
    doc.getVersionHash() shouldBe versionHash
    doc.getFileRelativePath() shouldBe "1.txt"
  }

  it should "join multi-segment relative paths correctly" in {
    val uri = new URI(s"dataset:///my_repo/$versionHash/some/nested/dir/data.csv")
    val doc = datasetDoc(uri)

    doc.getRepositoryName() shouldBe "my_repo"
    doc.getVersionHash() shouldBe versionHash
    doc.getFileRelativePath() shouldBe Paths.get("some", "nested", "dir", "data.csv").toString
  }

  it should "URL-decode the version hash" in {
    // FileResolver URL-encodes segments and then builds the URI with the multi-arg
    // constructor, so uri.getPath still contains URLEncoder-encoded segments.
    val uri = new URI("dataset", "", "/repo/hash%20with%2Bspecials/file.txt", null)
    val doc = datasetDoc(uri)

    doc.getVersionHash() shouldBe "hash with+specials"
    doc.getFileRelativePath() shouldBe "file.txt"
  }

  it should "URL-decode each relative path segment" in {
    val uri = new URI("dataset", "", "/repo/hash/dir+one/file%23two%20a.csv", null)
    val doc = datasetDoc(uri)

    doc.getRepositoryName() shouldBe "repo"
    doc.getVersionHash() shouldBe "hash"
    doc.getFileRelativePath() shouldBe Paths.get("dir one", "file#two a.csv").toString
  }

  it should "return the parsed components and the original URI through its getters" in {
    val uri = new URI("dataset", "", s"/repo/$versionHash/a%20b/c.csv", null)
    val doc = datasetDoc(uri)

    doc.getRepositoryName() shouldBe "repo"
    doc.getVersionHash() shouldBe versionHash
    doc.getFileRelativePath() shouldBe Paths.get("a b", "c.csv").toString
    // getURI must hand back the exact URI the document was constructed with.
    doc.getURI shouldBe uri
    doc.getURI.toString shouldBe uri.toString
  }

  it should "not URL-decode the repository name" in {
    // parseUri only URLDecoder-decodes the version hash and the relative-path
    // segments; the repository name is returned raw. This mirrors FileResolver,
    // which URLEncoder-encodes only the fileRelativePath segments. The multi-arg
    // URI constructor is required here: a single-arg URI already percent-decodes
    // getPath, so "%20" in a raw URI string would reach parseUri as a space.
    val uri = new URI("dataset", "", "/repo%20name/hash%20value/file.txt", null)
    val doc = datasetDoc(uri)

    doc.getRepositoryName() shouldBe "repo%20name"
    // Same encoded token in the version-hash position IS decoded (asymmetry pin).
    doc.getVersionHash() shouldBe "hash value"
    doc.getFileRelativePath() shouldBe "file.txt"
  }

  it should "round-trip non-ASCII UTF-8 relative path segments encoded FileResolver-style" in {
    val rawSegments = Seq("中文 目录", "中文 文件.csv")
    val encodedPath =
      rawSegments.map(URLEncoder.encode(_, StandardCharsets.UTF_8)).mkString("/")
    val uri = new URI("dataset", "", s"/repo/$versionHash/$encodedPath", null)
    val doc = datasetDoc(uri)

    doc.getFileRelativePath() shouldBe Paths.get(rawSegments.head, rawSegments.tail: _*).toString
  }

  it should "collapse redundant and trailing slashes in the URI path" in {
    // Paths.get collapses duplicate separators and ignores a trailing slash,
    // so this still yields exactly the three segments [repo, hash, file.txt].
    val doc = datasetDoc(new URI("dataset:///repo//hash///file.txt/"))

    doc.getRepositoryName() shouldBe "repo"
    doc.getVersionHash() shouldBe "hash"
    doc.getFileRelativePath() shouldBe "file.txt"
  }

  it should "preserve dot segments in the relative path without normalization (current behavior)" in {
    // "." and ".." segments are kept verbatim (current behavior): the relative
    // path is passed downstream un-normalized, with no sanitization applied.
    val parentDoc = datasetDoc(new URI("dataset:///repo/hash/../x.csv"))
    parentDoc.getFileRelativePath() shouldBe Paths.get("..", "x.csv").toString

    val dotDoc = datasetDoc(new URI("dataset:///repo/hash/./sub/../x.csv"))
    dotDoc.getFileRelativePath() shouldBe Paths.get(".", "sub", "..", "x.csv").toString
  }

  it should "reject URIs with fewer than three path segments" in {
    val invalidUris = Seq(
      new URI("dataset:///"), // 0 segments
      new URI("dataset:///repo"), // 1 segment
      new URI(s"dataset:///repo/$versionHash"), // 2 segments
      new URI("dataset:///repo/hash/") // trailing slash: still only 2 segments
    )
    invalidUris.foreach { uri =>
      val thrown = intercept[IllegalArgumentException] {
        datasetDoc(uri)
      }
      thrown.getMessage shouldBe "URI format is incorrect"
    }
  }

  // The companion object resolves the file-service endpoint from environment
  // variables, falling back to a trimmed default. This lazy val is read whenever
  // asInputStream needs to fetch a file; assert its fallback behavior without
  // requiring a live FileService or LakeFS. The check is guarded so it holds
  // regardless of whether the env override is present.
  "the companion" should
    "resolve the dataset presigned-URL endpoint, defaulting when the env override is absent" in {
    val expected =
      sys.env
        .getOrElse(
          EnvironmentalVariable.ENV_FILE_SERVICE_GET_DATASET_PRESIGNED_URL_ENDPOINT,
          "http://localhost:9092/api/dataset/presign-download"
        )
        .trim
    LakeFSFileDocument.presignEndpointOf(ResourceType.Dataset) shouldBe expected
  }

  // Each resource type resolves its own endpoint: a dataset grant must not authorize a model
  // file, so the two presign endpoints stay distinct.
  it should "resolve the model presigned-URL endpoint, defaulting when the env override is absent" in {
    val expected =
      sys.env
        .getOrElse(
          EnvironmentalVariable.ENV_FILE_SERVICE_GET_MODEL_PRESIGNED_URL_ENDPOINT,
          "http://localhost:9092/api/model/presign-download"
        )
        .trim
    LakeFSFileDocument.presignEndpointOf(ResourceType.Model) shouldBe expected
    LakeFSFileDocument.presignEndpointOf(ResourceType.Model) should not be
      LakeFSFileDocument.presignEndpointOf(ResourceType.Dataset)
  }

  // The user JWT token is shared by every LakeFS-backed document, whatever its resource type.
  it should "expose a trimmed user JWT token defaulting to empty" in {
    val expected =
      sys.env.getOrElse(EnvironmentalVariable.ENV_USER_JWT_TOKEN, "").trim
    LakeFSFileDocument.userJwtToken shouldBe expected
  }

  // -----------------------------------------------------------------------------------------
  // asInputStream
  // -----------------------------------------------------------------------------------------

  "asInputStream" should "read the bytes through the presigned URL, not through lakeFS" in {
    readFully(servedDoc()) shouldBe presignedContent

    // Exactly two hops, in order: the presign lookup, then the address it returned. The point of
    // presigning is that the bytes never travel through the lakeFS server, so a third hit on the
    // direct-download route would mean the optimization is not happening at all.
    hits.map(_.path) shouldEqual List(statPath, presignedPath)
    // The lookup must name this document's own repository/commit/file triple. Repository and
    // commit are pinned by the URL path above; the file is a query parameter.
    hits.head.query.get("path") shouldBe Some(fileName)
  }

  // Not asserted below: the WARN `fallbackToLakeFS` logs, which is the only trace an operator gets
  // that presigning silently degraded. workflow-core's test classpath has no SLF4J provider — the
  // run prints "No SLF4J providers were found", every logger is a NOP, and scala-logging's
  // isWarnEnabled guard means the call is not even evaluated — so a capturing appender has nothing
  // to attach to. Pinning it would take a logback-classic test dependency for the whole module.
  it should "fall back to a direct lakeFS download when the presign lookup fails" in {
    failing = Set(statPath)

    // A lakeFS deployment with presigning disabled (or an object store that refuses to sign)
    // must still yield the file rather than failing the read.
    readFully(servedDoc()) shouldBe fallbackContent

    hits.map(_.path) shouldEqual List(statPath, objectPath)
    hits.last.query.get("path") shouldBe Some(fileName)
  }

  it should "fall back to a direct lakeFS download when the presigned URL cannot be read" in {
    failing = Set(presignedPath)

    // Distinct from the case above: here the lookup succeeded and the *address* is unusable — an
    // expired signature, or an object store the JVM cannot reach. The fallback covers both, so a
    // stale presigned URL is not a read failure.
    readFully(servedDoc()) shouldBe fallbackContent

    hits.map(_.path) shouldEqual List(statPath, presignedPath, objectPath)
  }

  it should "name the whole nested relative path on both hops, not the file's last segment" in {
    failing = Set(statPath)

    // A nested file driven through the fallback puts the relative path on both hops of one read —
    // the presign lookup's query, then the direct download's — so this pins both places the read
    // path passes the file. Asking for "records.csv" alone would address a different object, one at
    // the repository root, which for most files does not exist: a silent 404 on both hops.
    readFully(nestedDoc()) shouldBe fallbackContent

    hits.map(_.path) shouldEqual List(statPath, objectPath)
    hits.map(_.query.get("path")) shouldEqual
      List(Some(nestedRelativePath), Some(nestedRelativePath))
  }

  // -----------------------------------------------------------------------------------------
  // asFile / clear
  // -----------------------------------------------------------------------------------------

  "asFile" should "copy the fetched bytes to a temp file and reuse that file on later calls" in {
    val doc = servedDoc()
    val first = doc.asFile()
    try {
      // The payload is longer than the 1024-byte copy buffer and not a multiple of it, so this
      // pins the buffered copy loop: writing a whole buffer on the final short read would append
      // trailing garbage, and stopping after one buffer would truncate.
      new String(Files.readAllBytes(first.toPath), StandardCharsets.UTF_8) shouldBe presignedContent

      // Callers hand the File to operator code that opens it more than once, so the document must
      // memoize it: re-materializing would download the file again and leak the earlier temp file.
      requests.clear()
      assert(doc.asFile() eq first)
      // Same File *and* nothing on the wire: re-fetching into the memoized path would keep the
      // identity assertion above green while re-downloading the whole object on every call.
      hits shouldBe empty
    } finally Files.deleteIfExists(first.toPath)
  }

  it should "close the stream it copied from" in {
    // A leaked stream here is a leaked socket per versioned-file read in a long-running worker
    // (the shape of the Iceberg reader leak, #6882). The HTTP stub cannot observe the close, so
    // hand asFile a stream that records it: asInputStream is overridable and asFile calls it
    // virtually, which is exactly the seam production uses.
    var closes = 0
    val doc = new LakeFSFileDocument(
      new URI(s"dataset:///$repositoryName/$versionHash/$fileName"),
      ResourceType.Dataset
    ) {
      override def asInputStream(): InputStream =
        new ByteArrayInputStream(presignedContent.getBytes(StandardCharsets.UTF_8)) {
          override def close(): Unit = {
            closes += 1
            super.close()
          }
        }
    }

    val file = doc.asFile()
    try {
      new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8) shouldBe presignedContent
      closes shouldBe 1
    } finally Files.deleteIfExists(file.toPath)
  }

  "clear" should "delete the materialized temp file and let a later asFile re-materialize it" in {
    val doc = servedDoc()
    val first = doc.asFile()
    // A *materialized* file, not merely the empty placeholder Files.createTempFile leaves behind:
    // existence alone is the JDK's guarantee, and the precondition this test destroys and rebuilds
    // is that the download landed on disk.
    Files.size(first.toPath) shouldBe presignedContent.length.toLong

    doc.clear()
    // The temp file lives in the OS temp directory for the life of the JVM otherwise; leaking one
    // per read is how a long-running worker fills its disk.
    Files.exists(first.toPath) shouldBe false

    // clear also drops the memo, so the document stays usable instead of handing back a File that
    // no longer exists.
    val second = doc.asFile()
    try {
      second.getPath should not be first.getPath
      new String(Files.readAllBytes(second.toPath), StandardCharsets.UTF_8) shouldBe
        presignedContent
    } finally Files.deleteIfExists(second.toPath)
  }

  it should "do nothing when no temp file has been materialized" in {
    val doc = servedDoc()
    // clear() is called on every document at the end of a workflow, materialized or not.
    noException should be thrownBy doc.clear()

    doc.asFile()
    doc.clear()
    // A second clear must not retry the delete on the path the first one removed, which would
    // throw NoSuchFileException.
    noException should be thrownBy doc.clear()
  }
}
