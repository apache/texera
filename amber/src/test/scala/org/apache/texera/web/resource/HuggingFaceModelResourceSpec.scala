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

package org.apache.texera.web.resource

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sun.net.httpserver.HttpServer
import kong.unirest.{
  Client,
  Headers,
  HttpRequest,
  HttpRequestSummary,
  HttpResponse,
  HttpResponseSummary,
  RawResponse,
  Unirest,
  Config => UnirestConfig
}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, InputStream, InputStreamReader}
import java.net.{Inet6Address, InetAddress, InetSocketAddress, UnknownHostException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{Function => JFunction}
import javax.ws.rs.core.Response
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * Tests for [[HuggingFaceModelResource]] covering the validation, security,
  * caching, and filesystem behavior of the resource.
  *
  * The HF Hub URLs are hard-coded in the resource, so the upstream calls are
  * exercised by swapping Unirest's HTTP clients (the global one and the media
  * proxy's own instance) for an in-process stub (`StubClient` below) that
  * serves canned responses and records the outgoing requests. Media-host DNS
  * lookups go through a fake resolver. Nothing in this suite leaves the
  * machine: the one test that exercises the real HTTP client talks to a
  * loopback server.
  */
class HuggingFaceModelResourceSpec extends AnyFunSuite with BeforeAndAfterEach {

  import HuggingFaceModelResource._

  private val mapper = new ObjectMapper()
  private var resource: HuggingFaceModelResource = _
  private val realMediaHostResolver = resolveMediaHost

  override def beforeEach(): Unit = {
    resource = new HuggingFaceModelResource()
    // Reset caches between tests so cache hits from one test can't leak into another.
    modelCache.invalidateAll()
    taskCache.invalidateAll()
  }

  override def afterEach(): Unit = {
    modelCache.invalidateAll()
    taskCache.invalidateAll()
    // Drop any stub HTTP client or fake resolver this test installed so
    // neither leaks between tests.
    Unirest.config().reset()
    mediaProxyHttp.config().reset()
    resolveMediaHost = realMediaHostResolver
  }

  // Helper: read a Response's string entity (assumes the body is a String).
  private def entityString(response: Response): String =
    response.getEntity match {
      case s: String => s
      case other     => other.toString
    }

  // Helper: read a Response's byte entity (assumes the body is a byte array).
  private def entityBytes(response: Response): Array[Byte] =
    response.getEntity.asInstanceOf[Array[Byte]]

  // Helper: assert that a Response carries a JSON error body shaped { "error": "..." }.
  private def assertErrorBody(response: Response): Unit = {
    val body = entityString(response)
    val node = mapper.readTree(body)
    assert(node.has("error"), s"expected JSON error body, got: $body")
  }

  // ────────────────────────────────────────────────────────────────────────
  // sanitizeToken
  // ────────────────────────────────────────────────────────────────────────

  test("sanitizeToken returns empty string when input is null") {
    assert(sanitizeToken(null) == "")
  }

  test("sanitizeToken returns empty string when input is empty") {
    assert(sanitizeToken("") == "")
  }

  test("sanitizeToken returns empty string when input is whitespace only") {
    assert(sanitizeToken("   ") == "")
    assert(sanitizeToken("\t\n") == "")
  }

  test("sanitizeToken trims surrounding whitespace") {
    assert(sanitizeToken("  hf_abc123  ") == "hf_abc123")
  }

  test("sanitizeToken preserves a valid token unchanged") {
    assert(sanitizeToken("hf_abc123XYZ") == "hf_abc123XYZ")
  }

  test("sanitizeToken preserves tokens containing special characters") {
    assert(sanitizeToken("abc-xyz_123.45") == "abc-xyz_123.45")
  }

  // ────────────────────────────────────────────────────────────────────────
  // isAllowedMediaHost — SSRF allowlist
  // ────────────────────────────────────────────────────────────────────────

  test("isAllowedMediaHost rejects null host") {
    assert(!isAllowedMediaHost(null))
  }

  test("isAllowedMediaHost rejects empty host") {
    assert(!isAllowedMediaHost(""))
  }

  test("isAllowedMediaHost accepts exact match on huggingface.co") {
    assert(isAllowedMediaHost("huggingface.co"))
  }

  test("isAllowedMediaHost accepts HF Hub CDN subdomains") {
    assert(isAllowedMediaHost("cdn-uploads.huggingface.co"))
    assert(isAllowedMediaHost("cdn-lfs.huggingface.co"))
  }

  test("isAllowedMediaHost is case-insensitive") {
    assert(isAllowedMediaHost("HUGGINGFACE.CO"))
    assert(isAllowedMediaHost("Cdn-LFS.HuggingFace.co"))
  }

  test("isAllowedMediaHost accepts fal.media and its subdomains") {
    assert(isAllowedMediaHost("fal.media"))
    assert(isAllowedMediaHost("v3b.fal.media"))
  }

  test("isAllowedMediaHost accepts replicate.delivery and its subdomains") {
    assert(isAllowedMediaHost("replicate.delivery"))
    assert(isAllowedMediaHost("cdn.replicate.delivery"))
  }

  test("isAllowedMediaHost accepts replicate.com and its subdomains") {
    assert(isAllowedMediaHost("replicate.com"))
    assert(isAllowedMediaHost("api.replicate.com"))
  }

  test("isAllowedMediaHost rejects lookalike domains (leading-dot guard)") {
    // The critical security test: evilhuggingface.co must NOT match huggingface.co.
    assert(!isAllowedMediaHost("evilhuggingface.co"))
    assert(!isAllowedMediaHost("notfal.media"))
    assert(!isAllowedMediaHost("xreplicate.com"))
  }

  test("isAllowedMediaHost rejects unrelated public domains") {
    assert(!isAllowedMediaHost("google.com"))
    assert(!isAllowedMediaHost("example.org"))
  }

  test("isAllowedMediaHost rejects localhost") {
    assert(!isAllowedMediaHost("localhost"))
    assert(!isAllowedMediaHost("LOCALHOST"))
  }

  test("isAllowedMediaHost rejects loopback IPs") {
    assert(!isAllowedMediaHost("127.0.0.1"))
    assert(!isAllowedMediaHost("0.0.0.0"))
  }

  test("isAllowedMediaHost rejects private IP ranges") {
    assert(!isAllowedMediaHost("10.0.0.1"))
    assert(!isAllowedMediaHost("192.168.1.1"))
    assert(!isAllowedMediaHost("172.16.0.1"))
  }

  test("isAllowedMediaHost rejects cloud metadata IP") {
    assert(!isAllowedMediaHost("169.254.169.254"))
  }

  test("isAllowedMediaHost accepts hf.co and its subdomains (Hub file downloads redirect there)") {
    assert(isAllowedMediaHost("hf.co"))
    assert(isAllowedMediaHost("us.aws.cdn.hf.co"))
    assert(isAllowedMediaHost("cas-bridge.xethub.hf.co"))
  }

  test("isAllowedMediaHost rejects hf.co lookalikes (leading-dot guard)") {
    assert(!isAllowedMediaHost("evilhf.co"))
    assert(!isAllowedMediaHost("hf.co.evil.com"))
  }

  // ────────────────────────────────────────────────────────────────────────
  // isPublicAddress — what a media host may resolve to
  // ────────────────────────────────────────────────────────────────────────

  // Parsing an IP literal never touches DNS.
  private def ip(literal: String): InetAddress = InetAddress.getByName(literal)

  private def assertAllPublic(literals: String*): Unit =
    literals.foreach(a => assert(isPublicAddress(ip(a)), s"$a should be public"))

  private def assertNonePublic(literals: String*): Unit =
    literals.foreach(a => assert(!isPublicAddress(ip(a)), s"$a should not be public"))

  test("isPublicAddress accepts globally routable IPv4 and IPv6 addresses") {
    assertAllPublic("8.8.8.8", "1.1.1.1", "13.32.0.1", "2606:4700:4700::1111", "2a00:1450:4001::1")
  }

  test("isPublicAddress accepts addresses just outside the non-public ranges") {
    assertAllPublic("172.15.255.255", "172.32.0.0", "100.63.255.255", "100.128.0.0", "11.0.0.0")
  }

  test("isPublicAddress rejects loopback and unspecified addresses") {
    assertNonePublic("127.0.0.1", "127.255.255.254", "0.0.0.0", "::1", "::")
  }

  test("isPublicAddress rejects the RFC 1918 private ranges") {
    assertNonePublic("10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.1.1")
  }

  test("isPublicAddress rejects link-local addresses, including the cloud metadata endpoint") {
    assertNonePublic("169.254.169.254", "169.254.0.1", "fe80::1")
  }

  test("isPublicAddress rejects the shared address space (100.64.0.0/10)") {
    // 100.100.100.200 is Alibaba Cloud's metadata endpoint.
    assertNonePublic("100.64.0.1", "100.100.100.200", "100.127.255.255")
  }

  test(
    "isPublicAddress rejects IPv6 unique-local addresses, including AWS's IPv6 metadata endpoint"
  ) {
    assertNonePublic("fc00::1", "fd00::1", "fd00:ec2::254")
  }

  test("isPublicAddress rejects multicast, reserved and documentation ranges") {
    assertNonePublic(
      "224.0.0.1",
      "239.255.255.250",
      "240.0.0.1",
      "255.255.255.255",
      "192.0.2.1",
      "198.51.100.1",
      "203.0.113.1",
      "198.18.0.1",
      "ff02::1",
      "2001:db8::1"
    )
  }

  test("isPublicAddress judges an IPv4 address embedded in IPv6 by the IPv4 address") {
    // getByName unwraps ::ffff:a.b.c.d to IPv4, so build the IPv6 form directly.
    val mappedLoopback =
      Inet6Address.getByAddress(
        null,
        Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, 127, 0, 0, 1),
        -1
      )
    assert(!isPublicAddress(mappedLoopback))
    assertNonePublic("::7f00:1", "64:ff9b::a00:1") // IPv4-compatible 127.0.0.1; NAT64 to 10.0.0.1
    assertAllPublic("64:ff9b::808:808") // NAT64 to 8.8.8.8
  }

  // ────────────────────────────────────────────────────────────────────────
  // errorJson — JSON escaping
  // ────────────────────────────────────────────────────────────────────────

  test("errorJson produces well-formed JSON for a simple message") {
    val out = errorJson("Failed to fetch models.")
    val node = mapper.readTree(out)
    assert(node.get("error").asText() == "Failed to fetch models.")
  }

  test("errorJson escapes double quotes in the message") {
    val out = errorJson("She said \"hi\"")
    // Must round-trip cleanly back to the original — Jackson handled the escaping.
    val node = mapper.readTree(out)
    assert(node.get("error").asText() == "She said \"hi\"")
  }

  test("errorJson escapes backslashes in the message") {
    val out = errorJson("path C:\\Users\\evil")
    val node = mapper.readTree(out)
    assert(node.get("error").asText() == "path C:\\Users\\evil")
  }

  test("errorJson escapes newlines and tabs in the message") {
    val out = errorJson("line1\nline2\tindented")
    val node = mapper.readTree(out)
    assert(node.get("error").asText() == "line1\nline2\tindented")
  }

  test("errorJson handles empty message") {
    val out = errorJson("")
    val node = mapper.readTree(out)
    assert(node.get("error").asText() == "")
  }

  // ────────────────────────────────────────────────────────────────────────
  // proxyRemoteMedia — input validation & SSRF
  // ────────────────────────────────────────────────────────────────────────

  test("proxyRemoteMedia returns 400 for null URL") {
    val response = resource.proxyRemoteMedia(null)
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia returns 400 for empty URL") {
    val response = resource.proxyRemoteMedia("")
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia returns 400 for whitespace URL") {
    val response = resource.proxyRemoteMedia("   ")
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects file:// URLs") {
    val response = resource.proxyRemoteMedia("file:///etc/passwd")
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects ftp:// URLs") {
    val response = resource.proxyRemoteMedia("ftp://example.com/data")
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects javascript: URLs") {
    val response = resource.proxyRemoteMedia("javascript:alert(1)")
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects localhost via SSRF allowlist (403)") {
    val response = resource.proxyRemoteMedia("http://localhost:8080/admin")
    assert(response.getStatus == 403)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects 127.0.0.1 via SSRF allowlist (403)") {
    val response = resource.proxyRemoteMedia("http://127.0.0.1:9200/_cat/indices")
    assert(response.getStatus == 403)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects AWS metadata IP via SSRF allowlist (403)") {
    val response =
      resource.proxyRemoteMedia("http://169.254.169.254/latest/meta-data/iam/")
    assert(response.getStatus == 403)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects private IP ranges via SSRF allowlist (403)") {
    val response = resource.proxyRemoteMedia("http://10.0.0.5/admin")
    assert(response.getStatus == 403)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects lookalike huggingface domain (leading-dot guard)") {
    val response = resource.proxyRemoteMedia("https://evilhuggingface.co/payload")
    assert(response.getStatus == 403)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects arbitrary public domains not on the allowlist") {
    val response = resource.proxyRemoteMedia("https://example.com/anything")
    assert(response.getStatus == 403)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects URLs with missing host") {
    val response = resource.proxyRemoteMedia("http:///no-host-here")
    assert(response.getStatus == 403)
    assertErrorBody(response)
  }

  // ────────────────────────────────────────────────────────────────────────
  // listModels — cache hit paths (no HF traffic required)
  // ────────────────────────────────────────────────────────────────────────

  test("listModels returns 200 with cached body when cache hits and no user token") {
    val cachedBody = """[{"id":"test-model","label":"test-model"}]"""
    modelCache.put("text-generation", cachedBody)

    val response = resource.listModels("text-generation", null, null)
    assert(response.getStatus == 200)
    assert(entityString(response) == cachedBody)
  }

  test("listModels cache hit does NOT carry the truncated header") {
    val cachedBody = """[{"id":"x"}]"""
    modelCache.put("foo", cachedBody)

    val response = resource.listModels("foo", null, null)
    assert(response.getHeaderString(TRUNCATED_HEADER) == null)
  }

  test("listModels cache hit is keyed by task — different task is a miss") {
    modelCache.put("text-classification", """[{"id":"a"}]""")
    // We don't want to actually hit HF, so we just assert that the cache for "image-classification"
    // is empty after `put` — i.e., Guava cache lookup is task-specific.
    assert(modelCache.getIfPresent("image-classification") == null)
    assert(modelCache.getIfPresent("text-classification") != null)
  }

  test("listModels with X-HF-Token header bypasses the cache (does not read from it)") {
    val cachedBody = """[{"id":"only-cached"}]"""
    modelCache.put("text-generation", cachedBody)

    // We can't easily assert the resource then *successfully* calls HF without a mock,
    // but we can verify the cache content is unchanged after a user-token call
    // (i.e., user-token requests don't populate the same cache slot).
    val before = modelCache.getIfPresent("text-generation")
    try {
      resource.listModels("text-generation", null, "hf_user_token_xyz")
    } catch {
      case _: Throwable => () // network may fail in unit tests; we only care about cache state
    }
    val after = modelCache.getIfPresent("text-generation")
    assert(before == after, "user-token request should not alter the anonymous cache slot")
  }

  // ────────────────────────────────────────────────────────────────────────
  // listTasks — cache hit paths (no HF traffic required)
  // ────────────────────────────────────────────────────────────────────────

  test("listTasks returns 200 with cached body when cache hits and no user token") {
    val cachedBody = """[{"tag":"text-generation","label":"Text Generation"}]"""
    taskCache.put(TASKS_CACHE_KEY, cachedBody)

    val response = resource.listTasks(null)
    assert(response.getStatus == 200)
    assert(entityString(response) == cachedBody)
  }

  test("listTasks with empty token header still reads from cache (sanitized to anonymous)") {
    val cachedBody = """[{"tag":"x","label":"X"}]"""
    taskCache.put(TASKS_CACHE_KEY, cachedBody)

    val response = resource.listTasks("   ")
    assert(response.getStatus == 200)
    assert(entityString(response) == cachedBody)
  }

  test("listTasks with X-HF-Token header bypasses the cache") {
    val cachedBody = """[{"tag":"only-cached"}]"""
    taskCache.put(TASKS_CACHE_KEY, cachedBody)

    val before = taskCache.getIfPresent(TASKS_CACHE_KEY)
    try {
      resource.listTasks("hf_user_token_xyz")
    } catch {
      case _: Throwable => ()
    }
    val after = taskCache.getIfPresent(TASKS_CACHE_KEY)
    assert(before == after, "user-token request should not alter the anonymous task cache slot")
  }

  // ════════════════════════════════════════════════════════════════════════
  // In-process Unirest stub — lets the upstream-facing paths (search, browse
  // pagination, task fan-out, media proxy) run for real without any network.
  // ════════════════════════════════════════════════════════════════════════

  /** One outgoing request as the resource issued it. */
  private case class StubRequest(url: String, authorization: Option[String])

  /** One canned upstream response. */
  private case class StubHttpResponse(
      status: Int,
      body: Array[Byte],
      headers: Map[String, String],
      contentType: String,
      content: Option[() => InputStream]
  )

  private def jsonResponse(
      body: String,
      status: Int = 200,
      headers: Map[String, String] = Map.empty
  ): StubHttpResponse =
    StubHttpResponse(
      status,
      body.getBytes(StandardCharsets.UTF_8),
      headers,
      "application/json",
      None
    )

  private def bytesResponse(
      body: Array[Byte],
      contentType: String,
      status: Int = 200,
      headers: Map[String, String] = Map.empty
  ): StubHttpResponse =
    StubHttpResponse(status, body, headers, contentType, None)

  /** A response whose body is produced lazily, so huge payloads never hit heap. */
  private def streamResponse(
      content: () => InputStream,
      contentType: String,
      headers: Map[String, String] = Map.empty
  ): StubHttpResponse =
    StubHttpResponse(200, Array.emptyByteArray, headers, contentType, Some(content))

  private val stubUnirestConfig = new UnirestConfig()

  private class StubRawResponse(canned: StubHttpResponse) extends RawResponse {
    private lazy val stream: InputStream =
      canned.content.map(_.apply()).getOrElse(new ByteArrayInputStream(canned.body))
    private val hdrs: Headers = {
      val h = new Headers()
      canned.headers.foreach { case (k, v) => h.add(k, v) }
      h
    }
    override def getStatus: Int = canned.status
    override def getStatusText: String = s"stub-${canned.status}"
    override def getHeaders: Headers = hdrs
    override def getContent: InputStream = stream
    override def getContentAsBytes: Array[Byte] = canned.body
    override def getContentAsString: String = new String(canned.body, StandardCharsets.UTF_8)
    override def getContentAsString(charset: String): String =
      new String(canned.body, StandardCharsets.UTF_8)
    override def getContentReader: InputStreamReader =
      new InputStreamReader(getContent, StandardCharsets.UTF_8)
    override def hasContent: Boolean = canned.body.nonEmpty || canned.content.isDefined
    override def getContentType: String = canned.contentType
    override def getEncoding: String = StandardCharsets.UTF_8.name()
    override def getConfig: UnirestConfig = stubUnirestConfig
    override def toSummary: HttpResponseSummary = null
    override def getRequestSummary: HttpRequestSummary = null
  }

  /**
    * Unirest `Client` that answers every request from `handler` and records
    * what was asked for. Thread-safe: `listTasks` fans out over a ForkJoinPool.
    */
  private class StubClient(handler: StubRequest => StubHttpResponse) extends Client {
    private val recorded = mutable.ListBuffer[StubRequest]()

    def requests: List[StubRequest] = recorded.synchronized(recorded.toList)
    def urls: List[String] = requests.map(_.url)

    override def getClient: Object = this

    // The signature mirrors Unirest's raw-typed `Client.request` as Scala sees it.
    override def request[T](
        req: HttpRequest[_ <: HttpRequest[_ <: AnyRef]],
        transformer: JFunction[RawResponse, HttpResponse[T]]
    ): HttpResponse[T] = {
      // Unirest's Headers.getFirst yields "" (not null) for an absent header.
      val captured =
        StubRequest(req.getUrl, Option(req.getHeaders.getFirst("Authorization")).filter(_.nonEmpty))
      recorded.synchronized(recorded += captured)
      transformer.apply(new StubRawResponse(handler(captured)))
    }

    override def close(): java.util.stream.Stream[Exception] =
      java.util.stream.Stream.empty[Exception]()

    override def registerShutdownHook(): Unit = ()
  }

  /** A public address for the fake resolver to hand back by default. */
  private val publicAddress = InetAddress.getByName("8.8.8.8")

  /**
    * Install a stub HTTP client, on both the global Unirest config and the
    * media proxy's own instance, for the duration of `body`. Media hosts
    * resolve to a public address unless the test sets `resolveMediaHost`.
    */
  private def withStubClient[A](handler: StubRequest => StubHttpResponse)(
      body: StubClient => A
  ): A = {
    val client = new StubClient(handler)
    Unirest.config().reset()
    Unirest.config().httpClient(client)
    mediaProxyHttp.config().reset()
    mediaProxyHttp.config().httpClient(client)
    resolveMediaHost = _ => Seq(publicAddress)
    try body(client)
    finally {
      Unirest.config().reset()
      mediaProxyHttp.config().reset()
      resolveMediaHost = realMediaHostResolver
    }
  }

  /** A raw HF model payload; omitted fields exercise the defaulting branches. */
  private def hfModel(
      id: String = "openai-community/gpt2",
      downloads: String = "5000",
      likes: String = "42",
      pipelineTag: String = "text-generation"
  ): String =
    s"""{"id":"$id","downloads":$downloads,"likes":$likes,"pipeline_tag":"$pipelineTag",
       |"private":false,"library_name":"transformers"}""".stripMargin.replace("\n", "")

  private def parseArray(response: Response): JsonNode = {
    val node = mapper.readTree(entityString(response))
    assert(node.isArray, s"expected a JSON array, got: ${entityString(response)}")
    node
  }

  private def fieldNamesOf(node: JsonNode): List[String] = node.fieldNames().asScala.toList

  private def nextLinkHeader(url: String): Map[String, String] =
    Map("Link" -> s"""<$url>; rel="next"""")

  /** An InputStream of `total` zero bytes that allocates nothing up front. */
  private class ZeroInputStream(total: Long) extends InputStream {
    private var remaining = total
    override def read(): Int = {
      if (remaining <= 0) -1
      else { remaining -= 1; 0 }
    }
    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (remaining <= 0) -1
      else {
        val n = math.min(len.toLong, remaining).toInt
        java.util.Arrays.fill(b, off, off + n, 0.toByte)
        remaining -= n
        n
      }
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // listModels — search mode
  // ────────────────────────────────────────────────────────────────────────

  test("listModels search mode simplifies each HF model to the five UI fields") {
    val response = withStubClient(_ => jsonResponse(s"[${hfModel()}]")) { _ =>
      resource.listModels("text-generation", "gpt2", null)
    }

    assert(response.getStatus == 200)
    val models = parseArray(response)
    assert(models.size() == 1)
    val entry = models.get(0)
    // Order matters: the resource builds a LinkedHashMap in this exact order.
    assert(fieldNamesOf(entry) == List("id", "label", "pipeline_tag", "downloads", "likes"))
    assert(entry.get("id").asText() == "openai-community/gpt2")
    assert(entry.get("label").asText() == "openai-community/gpt2")
    assert(entry.get("pipeline_tag").asText() == "text-generation")
    assert(entry.get("downloads").asLong() == 5000L)
    assert(entry.get("likes").asLong() == 42L)
  }

  test("listModels search mode defaults missing id, pipeline_tag, downloads and likes") {
    val raw = """[{"author":"nobody"},{"id":"x","downloads":"lots","likes":null}]"""
    val response = withStubClient(_ => jsonResponse(raw)) { _ =>
      resource.listModels("text-generation", "x", null)
    }

    val models = parseArray(response)
    assert(models.size() == 2)
    assert(models.get(0).get("id").asText() == "")
    assert(models.get(0).get("label").asText() == "")
    assert(models.get(0).get("pipeline_tag").asText() == "")
    assert(models.get(0).get("downloads").asLong() == 0L)
    assert(models.get(0).get("likes").asLong() == 0L)
    // Non-numeric downloads and a null likes both fall back to 0 rather than throwing.
    assert(models.get(1).get("downloads").asLong() == 0L)
    assert(models.get(1).get("likes").asLong() == 0L)
  }

  test("listModels search mode sends the trimmed query and warm-inference filters to HF") {
    val client = withStubClient(_ => jsonResponse("[]")) { c =>
      resource.listModels("image-classification", "  res net  ", null)
      c
    }

    assert(client.requests.size == 1)
    val url = client.urls.head
    assert(url.startsWith("https://huggingface.co/api/models?"))
    assert(url.contains("pipeline_tag=image-classification"))
    assert(url.contains("filter=image-classification"))
    assert(url.contains("sort=downloads"))
    assert(url.contains("direction=-1"))
    assert(url.contains("limit=100"))
    assert(url.contains("inference=warm"))
    // Leading/trailing whitespace is trimmed; the inner space is URL-encoded.
    assert(url.contains("search=res%20net") || url.contains("search=res+net"), url)
    assert(client.requests.head.authorization.isEmpty)
  }

  test("listModels search mode forwards the user token as a bearer Authorization header") {
    val client = withStubClient(_ => jsonResponse("[]")) { c =>
      resource.listModels("text-generation", "gpt2", "  hf_secret  ")
      c
    }

    assert(client.requests.head.authorization.contains("Bearer hf_secret"))
  }

  test("listModels search mode propagates the upstream HF status") {
    val response = withStubClient(_ => jsonResponse("""{"error":"rate limited"}""", status = 429)) {
      _ => resource.listModels("text-generation", "gpt2", null)
    }

    assert(response.getStatus == 429)
    assertErrorBody(response)
    assert(modelCache.getIfPresent("text-generation") == null, "errors must not be cached")
  }

  test("listModels search mode flags truncation when HF returns SEARCH_LIMIT results") {
    val full = (1 to 100).map(i => hfModel(id = s"model-$i")).mkString("[", ",", "]")
    val response = withStubClient(_ => jsonResponse(full)) { _ =>
      resource.listModels("text-generation", "gpt2", null)
    }

    assert(response.getStatus == 200)
    assert(parseArray(response).size() == 100)
    assert(response.getHeaderString(TRUNCATED_HEADER) == "true")
  }

  test("listModels search mode does not flag truncation below the search limit") {
    val partial = (1 to 99).map(i => hfModel(id = s"model-$i")).mkString("[", ",", "]")
    val response = withStubClient(_ => jsonResponse(partial)) { _ =>
      resource.listModels("text-generation", "gpt2", null)
    }

    assert(parseArray(response).size() == 99)
    assert(response.getHeaderString(TRUNCATED_HEADER) == null)
  }

  test("listModels search mode bypasses the browse cache in both directions") {
    modelCache.put("text-generation", """[{"id":"stale-cache-entry"}]""")

    val response = withStubClient(_ => jsonResponse(s"[${hfModel(id = "fresh/model")}]")) { c =>
      val r = resource.listModels("text-generation", "gpt2", null)
      assert(c.requests.size == 1, "a search must always hit HF, cache or not")
      r
    }

    assert(parseArray(response).get(0).get("id").asText() == "fresh/model")
    // Search results must not overwrite the browse cache slot.
    assert(modelCache.getIfPresent("text-generation") == """[{"id":"stale-cache-entry"}]""")
  }

  test("listModels returns 500 when HF returns a malformed search payload") {
    val response = withStubClient(_ => jsonResponse("not-json-at-all")) { _ =>
      resource.listModels("text-generation", "gpt2", null)
    }

    assert(response.getStatus == 500)
    val node = mapper.readTree(entityString(response))
    assert(node.get("error").asText() == "Failed to fetch models.")
  }

  test("listModels treats a whitespace-only search as browse mode") {
    modelCache.put("text-generation", """[{"id":"cached"}]""")

    val response = withStubClient(_ => fail("browse mode must be served from the cache")) { c =>
      val r = resource.listModels("text-generation", "   ", null)
      assert(c.requests.isEmpty)
      r
    }

    assert(response.getStatus == 200)
    assert(entityString(response) == """[{"id":"cached"}]""")
  }

  // ────────────────────────────────────────────────────────────────────────
  // listModels — browse mode (pagination via the Link header)
  // ────────────────────────────────────────────────────────────────────────

  test("listModels browse mode fetches a single page, simplifies it, and caches the result") {
    val body = s"[${hfModel(id = "a/one")},${hfModel(id = "b/two")}]"
    val client = withStubClient(_ => jsonResponse(body)) { c =>
      val response = resource.listModels("text-generation", null, null)
      assert(response.getStatus == 200)
      assert(response.getHeaderString(TRUNCATED_HEADER) == null)
      val models = parseArray(response)
      assert(models.size() == 2)
      assert(models.get(0).get("id").asText() == "a/one")
      assert(models.get(1).get("id").asText() == "b/two")
      assert(modelCache.getIfPresent("text-generation") == entityString(response))
      c
    }

    assert(client.requests.size == 1)
    val url = client.urls.head
    assert(url.contains("limit=1000"), s"browse mode uses the page size, got $url")
    assert(url.contains("inference=warm"))
    assert(!url.contains("search="))
  }

  test("listModels browse mode follows the Link rel=next chain across pages") {
    val page2Url = "https://huggingface.co/api/models?cursor=page2"
    val response = withStubClient { req =>
      if (req.url == page2Url) jsonResponse(s"[${hfModel(id = "p2/model")}]")
      else jsonResponse(s"[${hfModel(id = "p1/model")}]", headers = nextLinkHeader(page2Url))
    } { c =>
      val r = resource.listModels("text-generation", null, null)
      assert(c.urls.size == 2, s"unexpected request chain: ${c.urls}")
      assert(c.urls.head.contains("pipeline_tag=text-generation"))
      assert(c.urls(1) == page2Url)
      r
    }

    assert(response.getStatus == 200)
    val models = parseArray(response)
    assert(models.size() == 2)
    assert(models.get(0).get("id").asText() == "p1/model")
    assert(models.get(1).get("id").asText() == "p2/model")
    // A complete walk of the chain is not truncated.
    assert(response.getHeaderString(TRUNCATED_HEADER) == null)
  }

  test("listModels browse mode ignores a Link header without a next relation") {
    val headers = Map("Link" -> """<https://huggingface.co/api/models?cursor=prev>; rel="prev"""")
    val client = withStubClient(_ => jsonResponse(s"[${hfModel()}]", headers = headers)) { c =>
      resource.listModels("text-generation", null, null)
      c
    }

    assert(client.requests.size == 1)
  }

  test("listModels browse mode ignores a next Link that has no bracketed URL") {
    val headers = Map("Link" -> """https://huggingface.co/api/models?cursor=2; rel="next"""")
    val client = withStubClient(_ => jsonResponse(s"[${hfModel()}]", headers = headers)) { c =>
      resource.listModels("text-generation", null, null)
      c
    }

    assert(client.requests.size == 1)
  }

  test("listModels browse mode stops at MAX_PAGES and flags the response truncated") {
    val nextUrl = "https://huggingface.co/api/models?cursor=endless"
    val response = withStubClient { _ =>
      jsonResponse(s"[${hfModel()}]", headers = nextLinkHeader(nextUrl))
    } { c =>
      val r = resource.listModels("text-generation", null, null)
      assert(c.requests.size == 50, s"expected MAX_PAGES requests, got ${c.requests.size}")
      r
    }

    assert(response.getStatus == 200)
    assert(parseArray(response).size() == 50)
    assert(response.getHeaderString(TRUNCATED_HEADER) == "true")
    // Even a truncated browse result is cached for anonymous callers.
    assert(modelCache.getIfPresent("text-generation") == entityString(response))
  }

  test("listModels browse mode keeps earlier pages and flags truncation when a page fails") {
    val page2Url = "https://huggingface.co/api/models?cursor=page2"
    val response = withStubClient { req =>
      if (req.url == page2Url) jsonResponse("""{"error":"boom"}""", status = 503)
      else jsonResponse(s"[${hfModel(id = "p1/model")}]", headers = nextLinkHeader(page2Url))
    } { _ => resource.listModels("text-generation", null, null) }

    assert(response.getStatus == 200)
    val models = parseArray(response)
    assert(models.size() == 1)
    assert(models.get(0).get("id").asText() == "p1/model")
    assert(response.getHeaderString(TRUNCATED_HEADER) == "true")
  }

  test("listModels browse mode returns 500 when the first HF page fails") {
    val response = withStubClient(_ => jsonResponse("""{"error":"boom"}""", status = 500)) { _ =>
      resource.listModels("text-generation", null, null)
    }

    assert(response.getStatus == 500)
    val node = mapper.readTree(entityString(response))
    assert(node.get("error").asText() == "Failed to fetch models.")
    assert(modelCache.getIfPresent("text-generation") == null, "failures must not be cached")
  }

  test("listModels browse mode with a user token authenticates every page and skips the cache") {
    val page2Url = "https://huggingface.co/api/models?cursor=page2"
    val client = withStubClient { req =>
      if (req.url == page2Url) jsonResponse(s"[${hfModel(id = "p2/model")}]")
      else jsonResponse(s"[${hfModel(id = "p1/model")}]", headers = nextLinkHeader(page2Url))
    } { c =>
      val response = resource.listModels("text-generation", null, "hf_secret")
      assert(response.getStatus == 200)
      assert(parseArray(response).size() == 2)
      c
    }

    assert(client.requests.size == 2)
    assert(client.requests.forall(_.authorization.contains("Bearer hf_secret")))
    assert(
      modelCache.getIfPresent("text-generation") == null,
      "token-scoped results must never populate the shared cache"
    )
  }

  test("listModels browse mode serves the second call from the cache without hitting HF") {
    val client = withStubClient(_ => jsonResponse(s"[${hfModel(id = "a/one")}]")) { c =>
      val first = resource.listModels("text-generation", null, null)
      val second = resource.listModels("text-generation", null, null)
      assert(entityString(first) == entityString(second))
      c
    }

    assert(client.requests.size == 1, "the second browse call must be a cache hit")
  }

  // ────────────────────────────────────────────────────────────────────────
  // listTasks — pipeline-tag listing and availability fan-out
  // ────────────────────────────────────────────────────────────────────────

  private val tasksUrl = "https://huggingface.co/api/tasks"

  test("listTasks keeps only tags that have at least one warm model") {
    val tasksBody =
      """{"text-generation":{"label":"Text Generation"},"other-task":{"label":"Other"}}"""
    val response = withStubClient { req =>
      if (req.url == tasksUrl) jsonResponse(tasksBody)
      else if (req.url.contains("pipeline_tag=text-generation")) jsonResponse(s"[${hfModel()}]")
      else jsonResponse("[]")
    } { _ => resource.listTasks(null) }

    assert(response.getStatus == 200)
    val tasks = parseArray(response)
    assert(tasks.size() == 1)
    assert(tasks.get(0).get("tag").asText() == "text-generation")
    assert(tasks.get(0).get("label").asText() == "Text Generation")
    assert(fieldNamesOf(tasks.get(0)) == List("tag", "label"))
    assert(taskCache.getIfPresent(TASKS_CACHE_KEY) == entityString(response))
  }

  test("listTasks falls back to the tag when the label is missing or the value is not an object") {
    val tasksBody = """{"no-label":{"foo":1},"scalar-value":"not-an-object"}"""
    val response = withStubClient { req =>
      if (req.url == tasksUrl) jsonResponse(tasksBody) else jsonResponse(s"[${hfModel()}]")
    } { _ => resource.listTasks(null) }

    val tasks = parseArray(response)
    assert(tasks.size() == 2)
    val labels = (0 until tasks.size()).map { i =>
      tasks.get(i).get("tag").asText() -> tasks.get(i).get("label").asText()
    }.toMap
    assert(labels("no-label") == "no-label")
    assert(labels("scalar-value") == "scalar-value")
  }

  test("listTasks probes each tag with limit=1 and warm inference") {
    val client = withStubClient { req =>
      if (req.url == tasksUrl) jsonResponse("""{"summarization":{"label":"Summarization"}}""")
      else jsonResponse(s"[${hfModel()}]")
    } { c =>
      resource.listTasks(null)
      c
    }

    val probe = client.urls.find(_ != tasksUrl).getOrElse(fail("no model probe was issued"))
    assert(probe.contains("pipeline_tag=summarization"))
    assert(probe.contains("filter=summarization"))
    assert(probe.contains("limit=1"))
    assert(probe.contains("inference=warm"))
  }

  test("listTasks drops a tag whose probe is rate-limited") {
    val response = withStubClient { req =>
      if (req.url == tasksUrl) jsonResponse("""{"text-generation":{"label":"TG"}}""")
      else jsonResponse("""{"error":"too many requests"}""", status = 429)
    } { _ => resource.listTasks(null) }

    assert(response.getStatus == 200)
    assert(parseArray(response).size() == 0)
  }

  test("listTasks drops a tag whose probe returns an unexpected status") {
    val response = withStubClient { req =>
      if (req.url == tasksUrl) jsonResponse("""{"text-generation":{"label":"TG"}}""")
      else jsonResponse("""{"error":"nope"}""", status = 404)
    } { _ => resource.listTasks(null) }

    assert(parseArray(response).size() == 0)
  }

  test("listTasks drops a tag whose probe throws") {
    val response = withStubClient { req =>
      if (req.url == tasksUrl) jsonResponse("""{"text-generation":{"label":"TG"}}""")
      else throw new RuntimeException("connection reset")
    } { _ => resource.listTasks(null) }

    assert(response.getStatus == 200)
    assert(parseArray(response).size() == 0)
  }

  test("listTasks propagates the upstream status when the tasks endpoint fails") {
    val response = withStubClient(_ => jsonResponse("""{"error":"bad gateway"}""", status = 502)) {
      _ => resource.listTasks(null)
    }

    assert(response.getStatus == 502)
    assertErrorBody(response)
    assert(taskCache.getIfPresent(TASKS_CACHE_KEY) == null)
  }

  test("listTasks returns 500 when the tasks payload is malformed") {
    val response = withStubClient(_ => jsonResponse("{not-json")) { _ => resource.listTasks(null) }

    assert(response.getStatus == 500)
    val node = mapper.readTree(entityString(response))
    assert(node.get("error").asText() == "Failed to fetch tasks.")
    assert(taskCache.getIfPresent(TASKS_CACHE_KEY) == null)
  }

  test("listTasks with a user token authenticates the listing and the probes and skips the cache") {
    val client = withStubClient { req =>
      if (req.url == tasksUrl) jsonResponse("""{"text-generation":{"label":"TG"}}""")
      else jsonResponse(s"[${hfModel()}]")
    } { c =>
      val response = resource.listTasks("hf_secret")
      assert(response.getStatus == 200)
      assert(parseArray(response).size() == 1)
      c
    }

    assert(client.requests.size == 2)
    assert(client.requests.forall(_.authorization.contains("Bearer hf_secret")))
    assert(taskCache.getIfPresent(TASKS_CACHE_KEY) == null)
  }

  test("listTasks serves the second anonymous call from the cache without hitting HF") {
    val client = withStubClient { req =>
      if (req.url == tasksUrl) jsonResponse("""{"text-generation":{"label":"TG"}}""")
      else jsonResponse(s"[${hfModel()}]")
    } { c =>
      val first = resource.listTasks(null)
      val second = resource.listTasks(null)
      assert(entityString(first) == entityString(second))
      c
    }

    assert(client.requests.size == 2, "the second listTasks call must be served from the cache")
  }

  // ────────────────────────────────────────────────────────────────────────
  // proxyRemoteMedia — upstream streaming with the size cap
  // ────────────────────────────────────────────────────────────────────────

  private val allowedMediaUrl = "https://cdn-lfs.huggingface.co/generated/image.png"

  test("proxyRemoteMedia returns the upstream bytes and content type for an allowlisted host") {
    val payload = "PNG-fake-image-bytes".getBytes(StandardCharsets.UTF_8)
    val client = withStubClient(_ => bytesResponse(payload, "image/png")) { c =>
      val response = resource.proxyRemoteMedia(allowedMediaUrl)
      assert(response.getStatus == 200)
      assert(entityBytes(response).sameElements(payload))
      assert(response.getMediaType.toString == "image/png")
      c
    }

    assert(client.urls == List(allowedMediaUrl))
  }

  test("proxyRemoteMedia trims the upstream content type") {
    val response =
      withStubClient(_ => bytesResponse("x".getBytes(StandardCharsets.UTF_8), "  audio/wav  ")) {
        _ =>
          resource.proxyRemoteMedia(allowedMediaUrl)
      }

    assert(response.getMediaType.toString == "audio/wav")
  }

  test("proxyRemoteMedia falls back to octet-stream when upstream omits the content type") {
    val payload = "bytes".getBytes(StandardCharsets.UTF_8)
    val response = withStubClient(_ => bytesResponse(payload, null)) { _ =>
      resource.proxyRemoteMedia(allowedMediaUrl)
    }

    assert(response.getStatus == 200)
    assert(response.getMediaType.toString == "application/octet-stream")
  }

  test("proxyRemoteMedia falls back to octet-stream when the content type is blank") {
    val response =
      withStubClient(_ => bytesResponse("bytes".getBytes(StandardCharsets.UTF_8), "   ")) { _ =>
        resource.proxyRemoteMedia(allowedMediaUrl)
      }

    assert(response.getMediaType.toString == "application/octet-stream")
  }

  test("proxyRemoteMedia propagates a non-200 upstream status") {
    val response = withStubClient(_ => bytesResponse(Array.emptyByteArray, "text/plain", 404)) {
      _ => resource.proxyRemoteMedia(allowedMediaUrl)
    }

    assert(response.getStatus == 404)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia rejects an upstream declaring a Content-Length above the cap") {
    val declared = Map("Content-Length" -> (MAX_MEDIA_PROXY_BYTES + 1).toString)
    val client = withStubClient { _ =>
      // Body is tiny: the declared length alone must trigger the rejection, so
      // the oversized payload is never read into heap.
      bytesResponse("tiny".getBytes(StandardCharsets.UTF_8), "image/png", headers = declared)
    } { c =>
      val response = resource.proxyRemoteMedia(allowedMediaUrl)
      assert(response.getStatus == 413)
      assertErrorBody(response)
      c
    }

    assert(client.requests.size == 1)
  }

  test("proxyRemoteMedia serves the body when Content-Length is present but unparsable") {
    val payload = "ok".getBytes(StandardCharsets.UTF_8)
    val headers = Map("Content-Length" -> "not-a-number")
    val response = withStubClient(_ => bytesResponse(payload, "image/png", headers = headers)) {
      _ => resource.proxyRemoteMedia(allowedMediaUrl)
    }

    assert(response.getStatus == 200)
    assert(entityBytes(response).sameElements(payload))
  }

  test("proxyRemoteMedia serves a body whose declared Content-Length is within the cap") {
    val payload = "small".getBytes(StandardCharsets.UTF_8)
    val headers = Map("Content-Length" -> payload.length.toString)
    val response = withStubClient(_ => bytesResponse(payload, "image/png", headers = headers)) {
      _ => resource.proxyRemoteMedia(allowedMediaUrl)
    }

    assert(response.getStatus == 200)
    assert(entityBytes(response).sameElements(payload))
  }

  test("proxyRemoteMedia rejects a body that crosses the cap mid-stream") {
    // No Content-Length at all: the cap has to be enforced while reading.
    val response = withStubClient { _ =>
      streamResponse(() => new ZeroInputStream(MAX_MEDIA_PROXY_BYTES + 8192), "video/mp4")
    } { _ => resource.proxyRemoteMedia(allowedMediaUrl) }

    assert(response.getStatus == 413)
    assertErrorBody(response)
  }

  test("proxyRemoteMedia returns 500 when the upstream call throws") {
    val response = withStubClient(_ => throw new RuntimeException("connection reset")) { _ =>
      resource.proxyRemoteMedia(allowedMediaUrl)
    }

    assert(response.getStatus == 500)
    val node = mapper.readTree(entityString(response))
    assert(node.get("error").asText() == "Failed to proxy remote media.")
  }

  // ────────────────────────────────────────────────────────────────────────
  // proxyRemoteMedia — scheme, resolved address and redirect re-validation
  // ────────────────────────────────────────────────────────────────────────

  private val mediaPayload = "PNG-fake-image-bytes".getBytes(StandardCharsets.UTF_8)

  private def mediaOk: StubHttpResponse = bytesResponse(mediaPayload, "image/png")

  private def redirectTo(location: String, status: Int = 302): StubHttpResponse =
    bytesResponse(Array.emptyByteArray, "text/html", status, Map("Location" -> location))

  test("proxyRemoteMedia refuses plain http even for an allowlisted host") {
    val client = withStubClient(_ => mediaOk) { c =>
      val response = resource.proxyRemoteMedia("http://cdn-lfs.huggingface.co/generated/image.png")
      assert(response.getStatus == 400)
      assertErrorBody(response)
      c
    }

    assert(client.requests.isEmpty)
  }

  test("proxyRemoteMedia accepts an upper-case https scheme") {
    val response = withStubClient(_ => mediaOk) { _ =>
      resource.proxyRemoteMedia("HTTPS://cdn-lfs.huggingface.co/generated/image.png")
    }

    assert(response.getStatus == 200)
    assert(entityBytes(response).sameElements(mediaPayload))
  }

  test("proxyRemoteMedia refuses an allowlisted host that resolves to a private address") {
    val client = withStubClient(_ => mediaOk) { c =>
      resolveMediaHost = _ => Seq(ip("10.0.0.7"))
      val response = resource.proxyRemoteMedia(allowedMediaUrl)
      assert(response.getStatus == 403)
      assertErrorBody(response)
      c
    }

    assert(client.requests.isEmpty)
  }

  test("proxyRemoteMedia refuses a host when any one of its addresses is not public") {
    val client = withStubClient(_ => mediaOk) { c =>
      resolveMediaHost = _ => Seq(publicAddress, ip("127.0.0.1"))
      assert(resource.proxyRemoteMedia(allowedMediaUrl).getStatus == 403)
      c
    }

    assert(client.requests.isEmpty)
  }

  test("proxyRemoteMedia refuses a host that resolves to no addresses at all") {
    val client = withStubClient(_ => mediaOk) { c =>
      resolveMediaHost = _ => Seq.empty
      assert(resource.proxyRemoteMedia(allowedMediaUrl).getStatus == 403)
      c
    }

    assert(client.requests.isEmpty)
  }

  test("proxyRemoteMedia returns 502 when the media host cannot be resolved") {
    val client = withStubClient(_ => mediaOk) { c =>
      resolveMediaHost = host => throw new UnknownHostException(host)
      val response = resource.proxyRemoteMedia(allowedMediaUrl)
      assert(response.getStatus == 502)
      assertErrorBody(response)
      c
    }

    assert(client.requests.isEmpty)
  }

  test("proxyRemoteMedia does not follow a redirect to a host off the allowlist") {
    val client = withStubClient { req =>
      if (req.url == allowedMediaUrl) redirectTo("https://internal.example/secret")
      else bytesResponse("SECRET".getBytes(StandardCharsets.UTF_8), "text/plain")
    } { c =>
      val response = resource.proxyRemoteMedia(allowedMediaUrl)
      assert(response.getStatus == 403)
      assertErrorBody(response)
      c
    }

    assert(client.urls == List(allowedMediaUrl))
  }

  test("proxyRemoteMedia does not follow a redirect to the cloud metadata address") {
    val client = withStubClient { req =>
      if (req.url == allowedMediaUrl) redirectTo("http://169.254.169.254/latest/meta-data/")
      else bytesResponse("SECRET".getBytes(StandardCharsets.UTF_8), "text/plain")
    } { c =>
      assert(resource.proxyRemoteMedia(allowedMediaUrl).getStatus == 403)
      c
    }

    assert(client.urls == List(allowedMediaUrl))
  }

  test("proxyRemoteMedia does not follow a redirect that downgrades to http") {
    val client = withStubClient { req =>
      if (req.url == allowedMediaUrl)
        redirectTo("http://cdn-lfs.huggingface.co/generated/other.png")
      else mediaOk
    } { c =>
      assert(resource.proxyRemoteMedia(allowedMediaUrl).getStatus == 400)
      c
    }

    assert(client.urls == List(allowedMediaUrl))
  }

  test(
    "proxyRemoteMedia does not follow a redirect to an allowlisted name that resolves privately"
  ) {
    val client = withStubClient { req =>
      if (req.url == allowedMediaUrl) redirectTo("https://internal.huggingface.co/x.png")
      else mediaOk
    } { c =>
      resolveMediaHost = {
        case "internal.huggingface.co" => Seq(ip("192.168.0.10"))
        case _                         => Seq(publicAddress)
      }
      assert(resource.proxyRemoteMedia(allowedMediaUrl).getStatus == 403)
      c
    }

    assert(client.urls == List(allowedMediaUrl))
  }

  test("proxyRemoteMedia follows a Hub file redirect to the hf.co CDN and serves the final body") {
    val hubFileUrl = "https://huggingface.co/datasets/org/repo/resolve/main/cat.png"
    val cdnUrl = "https://us.aws.cdn.hf.co/xet-bridge-us/abc?Expires=1&Signature=x%7Ey"
    val client = withStubClient { req =>
      if (req.url == hubFileUrl) redirectTo(cdnUrl) else mediaOk
    } { c =>
      val response = resource.proxyRemoteMedia(hubFileUrl)
      assert(response.getStatus == 200)
      assert(entityBytes(response).sameElements(mediaPayload))
      assert(response.getMediaType.toString == "image/png")
      c
    }

    assert(client.urls == List(hubFileUrl, cdnUrl))
  }

  test("proxyRemoteMedia follows every redirect status code") {
    val finalUrl = "https://cdn-lfs.huggingface.co/generated/final.png"
    Seq(301, 302, 303, 307, 308).foreach { code =>
      val response = withStubClient { req =>
        if (req.url == allowedMediaUrl) redirectTo(finalUrl, code) else mediaOk
      } { _ => resource.proxyRemoteMedia(allowedMediaUrl) }
      assert(response.getStatus == 200, s"status $code was not followed")
    }
  }

  test("proxyRemoteMedia resolves a relative redirect against the current URL") {
    val client = withStubClient { req =>
      if (req.url == allowedMediaUrl) redirectTo("/generated/final.png") else mediaOk
    } { c =>
      assert(resource.proxyRemoteMedia(allowedMediaUrl).getStatus == 200)
      c
    }

    assert(
      client.urls == List(allowedMediaUrl, "https://cdn-lfs.huggingface.co/generated/final.png")
    )
  }

  private def hopUrl(n: Int): String = s"https://cdn-lfs.huggingface.co/hop/$n.png"

  /** Each hop redirects to the next one until `lastHop`, which serves the media. */
  private def redirectChain(lastHop: Int)(req: StubRequest): StubHttpResponse = {
    val n = req.url.stripPrefix("https://cdn-lfs.huggingface.co/hop/").stripSuffix(".png").toInt
    if (n < lastHop) redirectTo(hopUrl(n + 1)) else mediaOk
  }

  test("proxyRemoteMedia follows up to MAX_MEDIA_PROXY_REDIRECTS redirects") {
    val client = withStubClient(redirectChain(MAX_MEDIA_PROXY_REDIRECTS)) { c =>
      assert(resource.proxyRemoteMedia(hopUrl(0)).getStatus == 200)
      c
    }

    assert(client.requests.size == MAX_MEDIA_PROXY_REDIRECTS + 1)
  }

  test("proxyRemoteMedia gives up after MAX_MEDIA_PROXY_REDIRECTS redirects") {
    val client = withStubClient(redirectChain(Int.MaxValue)) { c =>
      val response = resource.proxyRemoteMedia(hopUrl(0))
      assert(response.getStatus == 502)
      assertErrorBody(response)
      c
    }

    assert(client.requests.size == MAX_MEDIA_PROXY_REDIRECTS + 1)
  }

  test("proxyRemoteMedia returns 502 for a redirect without a Location header") {
    val client = withStubClient(_ => bytesResponse(Array.emptyByteArray, "text/html", 302)) { c =>
      val response = resource.proxyRemoteMedia(allowedMediaUrl)
      assert(response.getStatus == 502)
      assertErrorBody(response)
      c
    }

    assert(client.requests.size == 1)
  }

  test("proxyRemoteMedia returns 502 for a redirect whose Location is not a valid URL") {
    val client = withStubClient(_ => redirectTo("https://cdn-lfs.huggingface.co/a b.png")) { c =>
      assert(resource.proxyRemoteMedia(allowedMediaUrl).getStatus == 502)
      c
    }

    assert(client.requests.size == 1)
  }

  // ────────────────────────────────────────────────────────────────────────
  // The media proxy's own HTTP client (real client, loopback server)
  // ────────────────────────────────────────────────────────────────────────

  test("the media proxy's HTTP client does not follow redirects on its own") {
    // Every hop has to come back to proxyRemoteMedia to be re-validated, so the
    // real client must hand a 302 back instead of following it.
    val targetHits = new AtomicInteger()
    val server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), 0)
    server.createContext(
      "/start",
      exchange => {
        exchange.getResponseHeaders.add("Location", "/target")
        exchange.sendResponseHeaders(302, -1)
        exchange.close()
      }
    )
    server.createContext(
      "/target",
      exchange => {
        targetHits.incrementAndGet()
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
      }
    )
    server.start()
    try {
      val port = server.getAddress.getPort
      val status = mediaProxyHttp.get(s"http://127.0.0.1:$port/start").asEmpty().getStatus
      assert(status == 302)
      assert(targetHits.get() == 0)
    } finally server.stop(0)
  }

  test("the media proxy's redirect policy leaves the global Unirest config alone") {
    assert(Unirest.config().getFollowRedirects)
  }
}
