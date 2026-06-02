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

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.common.cache.{Cache, CacheBuilder}
import kong.unirest.Unirest
import org.slf4j.{Logger, LoggerFactory}

import java.io.InputStream
import java.net.URI
import java.nio.file.{Files, Path => NioPath, Paths}
import java.util.concurrent.{Callable, ForkJoinPool, TimeUnit}
import java.util.stream.Collectors
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.jdk.CollectionConverters._

/**
  * REST resource that proxies the Hugging Face Hub API for the HuggingFace operator.
  *
  *   - GET  /api/huggingface/models?task=…[&search=…]   browse or search HF models
  *   - GET  /api/huggingface/tasks                       list HF pipeline tags with hosted inference
  *   - POST /api/huggingface/upload-audio?filename=…     stream-upload an audio file
  *   - GET  /api/huggingface/audio-preview?path=…        stream back an uploaded audio file
  *   - GET  /api/huggingface/media-proxy?url=…           proxy an allowlisted remote media URL
  *
  * Token sourcing: the user supplies their own HF token via the `X-HF-Token`
  * request header (forwarded by the frontend from the operator's property
  * panel). If the header is absent, requests go to HF Hub anonymously —
  * HF serves public model/task lists at public rate limits without auth.
  * The browse cache is bypassed whenever a user token is supplied, so one
  * user's private-model visibility never leaks into another user's response.
  */
@Path("/huggingface")
@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
class HuggingFaceModelResource {

  import HuggingFaceModelResource._

  @GET
  @Path("/models")
  def listModels(
      @QueryParam("task") @DefaultValue("text-generation") task: String,
      @QueryParam("search") search: String,
      @HeaderParam("X-HF-Token") userToken: String
  ): Response = {
    try {
      val hfToken = sanitizeToken(userToken)
      val isUserToken = hfToken.nonEmpty

      // ── Search mode: forward query to HF Hub, return results directly ──
      if (search != null && search.trim.nonEmpty) {
        return fetchSearchResults(task, search.trim, hfToken)
      }

      // ── Browse mode: return ALL models for this task ──
      // Only cache anonymous results, so a user with private-model visibility
      // can't have their token-scoped list served to a different user.
      if (!isUserToken) {
        val cached = modelCache.getIfPresent(task)
        if (cached != null) {
          return Response.ok(cached).build()
        }
      }

      val pageResult = fetchAllModelsForTask(task, hfToken)
      val json = objectMapper.writeValueAsString(pageResult.models)
      if (!isUserToken) modelCache.put(task, json)

      val builder = Response.ok(json)
      if (pageResult.truncated) builder.header(TRUNCATED_HEADER, "true")
      builder.build()
    } catch {
      case e: Exception =>
        logger.error(s"Failed to fetch HF models for task '$task'", e)
        errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed to fetch models.")
    }
  }

  /**
    * Streams an audio file from the request body to a temp file under
    * `${java.io.tmpdir}/texera-hf-audio`. Enforces an extension allowlist
    * and a max payload size (rejected with 413 once exceeded). Old files
    * in the temp dir are best-effort cleaned on each upload.
    */
  @POST
  @Path("/upload-audio")
  @Consumes(Array(MediaType.WILDCARD))
  def uploadAudioReference(
      @QueryParam("filename") filename: String,
      stream: InputStream
  ): Response = {
    try {
      if (stream == null) {
        return errorResponse(Response.Status.BAD_REQUEST, "Audio payload is required.")
      }

      val safeFileName = Option(filename)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(name => Paths.get(name).getFileName.toString)
        .getOrElse("audio.bin")
      val extension = {
        val idx = safeFileName.lastIndexOf('.')
        if (idx >= 0 && idx < safeFileName.length - 1)
          safeFileName.substring(idx).toLowerCase
        else ""
      }
      if (!ALLOWED_AUDIO_EXTENSIONS.contains(extension)) {
        return errorResponse(
          Response.Status.BAD_REQUEST,
          "Unsupported audio file extension."
        )
      }

      val tempDir = audioTempDir
      Files.createDirectories(tempDir)
      sweepOldAudioFiles(tempDir)

      val tempFile: NioPath = Files.createTempFile(tempDir, "hf-audio-", extension)
      tempFile.toFile.deleteOnExit()

      val out = Files.newOutputStream(tempFile)
      var totalBytes = 0L
      try {
        val buf = new Array[Byte](8 * 1024)
        var read = stream.read(buf)
        while (read != -1) {
          totalBytes += read
          if (totalBytes > MAX_AUDIO_BYTES) {
            out.close()
            Files.deleteIfExists(tempFile)
            return errorResponse(
              Response.Status.REQUEST_ENTITY_TOO_LARGE,
              "Audio payload exceeds the size limit."
            )
          }
          out.write(buf, 0, read)
          read = stream.read(buf)
        }
      } finally {
        out.close()
      }

      if (totalBytes == 0L) {
        Files.deleteIfExists(tempFile)
        return errorResponse(Response.Status.BAD_REQUEST, "Audio payload is empty.")
      }

      val json = objectMapper.writeValueAsString(
        Map(
          "path" -> tempFile.toAbsolutePath.toString,
          "fileName" -> safeFileName
        ).asJava
      )
      Response.ok(json).build()
    } catch {
      case e: Exception =>
        logger.error("Failed to upload audio", e)
        errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed to upload audio.")
    }
  }

  @GET
  @Path("/audio-preview")
  def previewUploadedAudio(@QueryParam("path") path: String): Response = {
    try {
      val trimmedPath = Option(path).map(_.trim).getOrElse("")
      if (trimmedPath.isEmpty) {
        return errorResponse(Response.Status.BAD_REQUEST, "Audio path is required.")
      }

      val tempDir = audioTempDir.toAbsolutePath.normalize()
      val requestedPath = Paths.get(trimmedPath).toAbsolutePath.normalize()
      if (!requestedPath.startsWith(tempDir)) {
        return errorResponse(
          Response.Status.FORBIDDEN,
          "Audio path is outside the allowed preview directory."
        )
      }
      if (!Files.exists(requestedPath) || !Files.isRegularFile(requestedPath)) {
        return errorResponse(Response.Status.NOT_FOUND, "Uploaded audio file was not found.")
      }

      // Defense-in-depth: even though /upload-audio enforces MAX_AUDIO_BYTES on
      // ingest, refuse to buffer an oversized file into heap on the response
      // side. Catches files placed via a future-bug or out-of-band write.
      val size = Files.size(requestedPath)
      if (size > MAX_AUDIO_BYTES) {
        logger.warn(
          s"Uploaded audio file size $size exceeds cap $MAX_AUDIO_BYTES; rejecting."
        )
        return errorResponse(
          Response.Status.REQUEST_ENTITY_TOO_LARGE,
          "Uploaded audio file exceeds the size limit."
        )
      }

      val contentType = Option(Files.probeContentType(requestedPath))
        .filter(_.trim.nonEmpty)
        .getOrElse(inferAudioContentType(requestedPath))
      Response.ok(Files.readAllBytes(requestedPath), contentType).build()
    } catch {
      case e: Exception =>
        logger.error("Failed to read uploaded audio", e)
        errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed to read uploaded audio.")
    }
  }

  /**
    * Proxies a remote media URL to bypass browser CORS for HF inference responses.
    * Only http(s) URLs whose host is in ALLOWED_MEDIA_HOST_SUFFIXES are accepted,
    * blocking SSRF probes against internal services.
    */
  @GET
  @Path("/media-proxy")
  def proxyRemoteMedia(@QueryParam("url") url: String): Response = {
    try {
      val trimmedUrl = Option(url).map(_.trim).getOrElse("")
      if (trimmedUrl.isEmpty) {
        return errorResponse(Response.Status.BAD_REQUEST, "Media URL is required.")
      }
      if (!trimmedUrl.startsWith("http://") && !trimmedUrl.startsWith("https://")) {
        return errorResponse(
          Response.Status.BAD_REQUEST,
          "Only http(s) media URLs are supported."
        )
      }

      val parsed =
        try { new URI(trimmedUrl) }
        catch { case _: Exception => null }
      if (parsed == null || parsed.getHost == null || !isAllowedMediaHost(parsed.getHost)) {
        return errorResponse(Response.Status.FORBIDDEN, "Media URL host is not allowed.")
      }

      // Stream the upstream response via asObject(Function<RawResponse, T>) so
      // we never have to materialise it into a heap-resident byte[] before we
      // can enforce the size cap. The function returns a MediaProxyOutcome
      // that this method then converts into a Jersey Response.
      val outcome = Unirest
        .get(trimmedUrl)
        .connectTimeout(CONNECT_TIMEOUT_MS)
        .socketTimeout(SOCKET_TIMEOUT_LONG_MS)
        .asObject((raw: kong.unirest.RawResponse) => streamMediaWithCap(raw))
        .getBody

      outcome match {
        case MediaProxyOk(bytes, contentType) =>
          Response.ok(bytes, contentType.getOrElse(MediaType.APPLICATION_OCTET_STREAM)).build()
        case MediaProxyError(status, message) =>
          errorResponse(status, message)
      }
    } catch {
      case e: Exception =>
        logger.error("Failed to proxy remote media", e)
        errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed to proxy remote media.")
    }
  }

  /**
    * Read an upstream media response with a hard size cap. Aborts early both
    * when the declared Content-Length exceeds the cap and when the body crosses
    * it mid-read (in case the upstream lies about Content-Length or omits it).
    */
  private def streamMediaWithCap(raw: kong.unirest.RawResponse): MediaProxyOutcome = {
    if (raw.getStatus != 200) {
      logger.warn(s"Upstream media fetch returned ${raw.getStatus}: ${raw.getStatusText}")
      return MediaProxyError(raw.getStatus, "Failed to fetch remote media.")
    }

    val declaredLength = Option(raw.getHeaders.getFirst("Content-Length"))
      .flatMap(s => scala.util.Try(s.trim.toLong).toOption)
    if (declaredLength.exists(_ > MAX_MEDIA_PROXY_BYTES)) {
      logger.warn(
        s"Upstream Content-Length ${declaredLength.get} exceeds cap $MAX_MEDIA_PROXY_BYTES; rejecting."
      )
      return MediaProxyError(
        Response.Status.REQUEST_ENTITY_TOO_LARGE.getStatusCode,
        "Remote media exceeds the size limit."
      )
    }

    val buffered = new java.io.ByteArrayOutputStream()
    val buf = new Array[Byte](8 * 1024)
    val in = raw.getContent
    var totalBytes = 0L
    var exceeded = false
    var read = in.read(buf)
    while (read != -1 && !exceeded) {
      totalBytes += read
      if (totalBytes > MAX_MEDIA_PROXY_BYTES) {
        exceeded = true
      } else {
        buffered.write(buf, 0, read)
        read = in.read(buf)
      }
    }
    if (exceeded) {
      logger.warn(s"Upstream media exceeded cap $MAX_MEDIA_PROXY_BYTES mid-stream; rejecting.")
      return MediaProxyError(
        Response.Status.REQUEST_ENTITY_TOO_LARGE.getStatusCode,
        "Remote media exceeds the size limit."
      )
    }

    val contentType = Option(raw.getContentType).map(_.trim).filter(_.nonEmpty)
    MediaProxyOk(buffered.toByteArray, contentType)
  }

  /** Search HF Hub for models matching a query within a task. */
  private def fetchSearchResults(task: String, query: String, hfToken: String): Response = {
    var request = Unirest
      .get("https://huggingface.co/api/models")
      .queryString("pipeline_tag", task)
      .queryString("sort", "downloads")
      .queryString("direction", "-1")
      .queryString("limit", SEARCH_LIMIT.toString)
      .queryString("filter", task)
      .queryString("inference", "warm")
      .queryString("search", query)
      .connectTimeout(CONNECT_TIMEOUT_MS)
      .socketTimeout(SOCKET_TIMEOUT_MS)

    if (hfToken.nonEmpty) {
      request = request.header("Authorization", s"Bearer $hfToken")
    }

    val hfResponse = request.asString()

    if (hfResponse.getStatus != 200) {
      logger.warn(
        s"HF search returned ${hfResponse.getStatus}: ${hfResponse.getStatusText}"
      )
      return errorResponse(hfResponse.getStatus, "Hugging Face API error.")
    }

    val rawModels = objectMapper.readValue(hfResponse.getBody, listOfMapsType)
    val out = buildSimplifiedList(rawModels)
    val truncated = rawModels.size() >= SEARCH_LIMIT
    val builder = Response.ok(objectMapper.writeValueAsString(out))
    if (truncated) builder.header(TRUNCATED_HEADER, "true")
    builder.build()
  }

  /** GET /api/huggingface/tasks — list HF pipeline tags that have models with hosted inference. */
  @GET
  @Path("/tasks")
  def listTasks(@HeaderParam("X-HF-Token") userToken: String): Response = {
    try {
      val hfToken = sanitizeToken(userToken)
      val isUserToken = hfToken.nonEmpty

      // Only cache anonymous results — see comment in listModels.
      if (!isUserToken) {
        val cached = taskCache.getIfPresent(TASKS_CACHE_KEY)
        if (cached != null) {
          return Response.ok(cached).build()
        }
      }

      var request = Unirest
        .get("https://huggingface.co/api/tasks")
        .connectTimeout(CONNECT_TIMEOUT_MS)
        .socketTimeout(SOCKET_TIMEOUT_MS)

      if (hfToken.nonEmpty) {
        request = request.header("Authorization", s"Bearer $hfToken")
      }

      val hfResponse = request.asString()

      if (hfResponse.getStatus != 200) {
        logger.warn(
          s"HF tasks endpoint returned ${hfResponse.getStatus}: ${hfResponse.getStatusText}"
        )
        return errorResponse(hfResponse.getStatus, "Hugging Face API error.")
      }

      // /api/tasks returns { "<pipeline_tag>": { "label": "...", ... }, ... }
      val root: JsonNode = objectMapper.readTree(hfResponse.getBody)
      val taskList = new java.util.ArrayList[java.util.Map[String, Object]]()
      val iter = root.fields()
      while (iter.hasNext) {
        val entry = iter.next()
        val tag = entry.getKey
        val info: JsonNode = entry.getValue
        val label =
          if (info != null && info.isObject && info.has("label")) info.get("label").asText(tag)
          else tag
        val taskEntry = new java.util.LinkedHashMap[String, Object]()
        taskEntry.put("tag", tag)
        taskEntry.put("label", label)
        taskList.add(taskEntry)
      }

      // Bounded fan-out: scope the parallelStream to our own ForkJoinPool
      // (size = TASK_FETCH_PARALLELISM) instead of the global common pool.
      val availableTasks =
        taskCheckPool
          .submit(new Callable[java.util.List[java.util.Map[String, Object]]] {
            override def call(): java.util.List[java.util.Map[String, Object]] = {
              taskList
                .parallelStream()
                .filter(t => hasModelsForTask(t.get("tag").toString, hfToken))
                .collect(Collectors.toList())
            }
          })
          .get()

      val json = objectMapper.writeValueAsString(availableTasks)
      if (!isUserToken) taskCache.put(TASKS_CACHE_KEY, json)
      Response.ok(json).build()
    } catch {
      case e: Exception =>
        logger.error("Failed to fetch HF tasks", e)
        errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed to fetch tasks.")
    }
  }

  /**
    * Fetch all models for a given task by paginating the HF Hub Link header.
    * Stops at MAX_PAGES pages; sets `truncated = true` if pagination stopped
    * early (either by hitting MAX_PAGES or an upstream error mid-pagination).
    */
  private def fetchAllModelsForTask(
      task: String,
      hfToken: String
  ): PageResult = {
    val allResults = new java.util.ArrayList[java.util.Map[String, Object]]()
    var nextUrl: String = null
    var pageCount = 0

    var request = Unirest
      .get("https://huggingface.co/api/models")
      .queryString("pipeline_tag", task)
      .queryString("sort", "downloads")
      .queryString("direction", "-1")
      .queryString("limit", PAGE_SIZE.toString)
      .queryString("filter", task)
      .queryString("inference", "warm")
      .connectTimeout(CONNECT_TIMEOUT_MS)
      .socketTimeout(SOCKET_TIMEOUT_MS)

    if (hfToken.nonEmpty) {
      request = request.header("Authorization", s"Bearer $hfToken")
    }

    var hfResponse = request.asString()

    if (hfResponse.getStatus != 200) {
      throw new RuntimeException(
        s"HF API returned ${hfResponse.getStatus} for task '$task'"
      )
    }

    var rawModels = objectMapper.readValue(hfResponse.getBody, listOfMapsType)
    allResults.addAll(buildSimplifiedList(rawModels))
    pageCount += 1

    nextUrl = extractNextLink(hfResponse.getHeaders.getFirst("Link"))

    while (nextUrl != null && pageCount < MAX_PAGES) {
      var nextRequest = Unirest
        .get(nextUrl)
        .connectTimeout(CONNECT_TIMEOUT_MS)
        .socketTimeout(SOCKET_TIMEOUT_MS)
      if (hfToken.nonEmpty) {
        nextRequest = nextRequest.header("Authorization", s"Bearer $hfToken")
      }

      hfResponse = nextRequest.asString()

      if (hfResponse.getStatus != 200) {
        logger.warn(
          s"HF pagination stopped early at page $pageCount for task '$task' with status ${hfResponse.getStatus}"
        )
        return PageResult(allResults, truncated = true)
      }

      rawModels = objectMapper.readValue(hfResponse.getBody, listOfMapsType)
      allResults.addAll(buildSimplifiedList(rawModels))
      pageCount += 1

      nextUrl = extractNextLink(hfResponse.getHeaders.getFirst("Link"))
    }

    val truncated = nextUrl != null && pageCount >= MAX_PAGES
    if (truncated) {
      logger.warn(s"HF pagination stopped at MAX_PAGES=$MAX_PAGES for task '$task'")
    }

    PageResult(allResults, truncated)
  }

  /**
    * Parse the Link header to extract the URL with rel="next".
    * Format: <https://huggingface.co/api/models?...>; rel="next"
    */
  private def extractNextLink(linkHeader: String): String = {
    if (linkHeader == null || linkHeader.isEmpty) return null

    val parts = linkHeader.split(",")
    for (part <- parts) {
      val trimmed = part.trim
      if (trimmed.contains("rel=\"next\"")) {
        val start = trimmed.indexOf('<')
        val end = trimmed.indexOf('>')
        if (start >= 0 && end > start) {
          return trimmed.substring(start + 1, end)
        }
      }
    }
    null
  }

  /**
    * Returns true if at least one model exists for the given task with hosted inference.
    * Logs 429/503 explicitly so callers can spot HF rate-limit pressure.
    */
  private def hasModelsForTask(task: String, hfToken: String): Boolean = {
    try {
      var request = Unirest
        .get("https://huggingface.co/api/models")
        .queryString("pipeline_tag", task)
        .queryString("filter", task)
        .queryString("limit", "1")
        .queryString("inference", "warm")
        .connectTimeout(CONNECT_TIMEOUT_SHORT_MS)
        .socketTimeout(SOCKET_TIMEOUT_SHORT_MS)

      if (hfToken.nonEmpty) {
        request = request.header("Authorization", s"Bearer $hfToken")
      }

      val response = request.asString()
      response.getStatus match {
        case 200 =>
          val models = objectMapper.readValue(response.getBody, listOfMapsType)
          !models.isEmpty
        case 429 | 503 =>
          logger.warn(
            s"HF rate-limit/unavailable (status ${response.getStatus}) when checking task '$task'"
          )
          false
        case other =>
          logger.debug(s"HF returned status $other when checking task '$task'")
          false
      }
    } catch {
      case e: Exception =>
        logger.debug(s"hasModelsForTask failed for '$task': ${e.getMessage}")
        false
    }
  }

  /** Convert raw HF model maps into simplified maps for the frontend. */
  private def buildSimplifiedList(
      rawModels: java.util.List[java.util.Map[String, Object]]
  ): java.util.List[java.util.Map[String, Object]] = {
    val out = new java.util.ArrayList[java.util.Map[String, Object]]()
    val iter = rawModels.iterator()
    while (iter.hasNext) {
      val model = iter.next()
      val id = if (model.get("id") != null) model.get("id").toString else ""
      val downloads: java.lang.Long = model.get("downloads") match {
        case n: java.lang.Number => n.longValue()
        case _                   => 0L
      }
      val likes: java.lang.Long = model.get("likes") match {
        case n: java.lang.Number => n.longValue()
        case _                   => 0L
      }
      val pipelineTag =
        if (model.get("pipeline_tag") != null) model.get("pipeline_tag").toString else ""

      val entry = new java.util.LinkedHashMap[String, Object]()
      entry.put("id", id)
      entry.put("label", id)
      entry.put("pipeline_tag", pipelineTag)
      entry.put("downloads", downloads)
      entry.put("likes", likes)
      out.add(entry)
    }
    out
  }
}

object HuggingFaceModelResource {
  private val logger: Logger = LoggerFactory.getLogger(classOf[HuggingFaceModelResource])

  private val objectMapper: ObjectMapper = new ObjectMapper()

  private val listOfMapsType =
    new TypeReference[java.util.List[java.util.Map[String, Object]]]() {}

  // ── Network timeouts (ms) ──
  private val CONNECT_TIMEOUT_MS = 10000
  private val SOCKET_TIMEOUT_MS = 30000
  private val CONNECT_TIMEOUT_SHORT_MS = 5000
  private val SOCKET_TIMEOUT_SHORT_MS = 10000
  private val SOCKET_TIMEOUT_LONG_MS = 120000

  // ── Pagination ──
  private val PAGE_SIZE = 1000
  private val MAX_PAGES = 50
  private val SEARCH_LIMIT = 100

  /** Response header set when a list response was truncated (server-side limit hit). */
  private[resource] val TRUNCATED_HEADER = "X-Texera-Truncated"

  // ── Caches: bounded with TTL ──
  private val MODEL_CACHE_MAX_SIZE = 100L
  private val MODEL_CACHE_TTL_MINUTES = 60L
  private val TASK_CACHE_MAX_SIZE = 8L
  private val TASK_CACHE_TTL_MINUTES = 60L

  private[resource] val modelCache: Cache[String, String] = CacheBuilder
    .newBuilder()
    .maximumSize(MODEL_CACHE_MAX_SIZE)
    .expireAfterWrite(MODEL_CACHE_TTL_MINUTES, TimeUnit.MINUTES)
    .build()

  private[resource] val taskCache: Cache[String, String] = CacheBuilder
    .newBuilder()
    .maximumSize(TASK_CACHE_MAX_SIZE)
    .expireAfterWrite(TASK_CACHE_TTL_MINUTES, TimeUnit.MINUTES)
    .build()

  private[resource] val TASKS_CACHE_KEY = "all"

  // ── /tasks fan-out throttle: bounded ForkJoinPool instead of the global common pool ──
  private val TASK_FETCH_PARALLELISM = 4
  private val taskCheckPool = new ForkJoinPool(TASK_FETCH_PARALLELISM)

  // ── /upload-audio constraints ──
  private[resource] val MAX_AUDIO_BYTES: Long = 25L * 1024L * 1024L // 25 MiB
  private[resource] val ALLOWED_AUDIO_EXTENSIONS: Set[String] =
    Set(".wav", ".mp3", ".mpeg", ".flac", ".ogg", ".oga", ".webm", ".opus", ".amr", ".m4a", ".aac")
  private[resource] val AUDIO_TEMP_TTL_MS: Long = 60L * 60L * 1000L // 1 hour

  // ── /media-proxy size cap: bounds the upstream response we buffer in heap ──
  // Sized to cover HF inference media outputs (text-to-image ~5 MiB,
  // text-to-video ~30 MiB) with headroom. Bumps should land in their own PR.
  private[resource] val MAX_MEDIA_PROXY_BYTES: Long = 50L * 1024L * 1024L // 50 MiB

  /** Outcome of streaming an upstream media response with the size cap. */
  private[resource] sealed trait MediaProxyOutcome
  private[resource] case class MediaProxyOk(bytes: Array[Byte], contentType: Option[String])
      extends MediaProxyOutcome
  private[resource] case class MediaProxyError(status: Int, message: String)
      extends MediaProxyOutcome

  // ── /media-proxy allowlist (SSRF protection) ──
  // Add new hosts here when integrating with a new HF inference provider.
  private val ALLOWED_MEDIA_HOST_SUFFIXES: Set[String] = Set(
    "huggingface.co",
    "fal.media",
    "replicate.delivery",
    "replicate.com"
  )

  private[resource] def audioTempDir: NioPath =
    Paths.get(System.getProperty("java.io.tmpdir"), "texera-hf-audio")

  /** Delete audio temp files older than AUDIO_TEMP_TTL_MS. Best-effort. */
  private[resource] def sweepOldAudioFiles(tempDir: NioPath): Unit = {
    val cutoff = System.currentTimeMillis() - AUDIO_TEMP_TTL_MS
    try {
      val stream = Files.list(tempDir)
      try {
        stream.forEach { p =>
          try {
            if (Files.isRegularFile(p) && Files.getLastModifiedTime(p).toMillis < cutoff) {
              Files.deleteIfExists(p)
            }
          } catch {
            case _: Exception => // skip files we can't stat/delete
          }
        }
      } finally {
        stream.close()
      }
    } catch {
      case e: Exception =>
        logger.debug(s"Audio temp dir sweep failed: ${e.getMessage}")
    }
  }

  /** Allow exact host or subdomain of any entry in ALLOWED_MEDIA_HOST_SUFFIXES. */
  private[resource] def isAllowedMediaHost(host: String): Boolean = {
    if (host == null || host.isEmpty) return false
    val lower = host.toLowerCase
    ALLOWED_MEDIA_HOST_SUFFIXES.exists(suffix => lower == suffix || lower.endsWith("." + suffix))
  }

  /** Trim and null-coalesce the X-HF-Token header value; empty means anonymous. */
  private[resource] def sanitizeToken(headerValue: String): String =
    Option(headerValue).map(_.trim).filter(_.nonEmpty).getOrElse("")

  /** Build a JSON error body using Jackson so the message is properly escaped. */
  private[resource] def errorJson(message: String): String =
    objectMapper.writeValueAsString(Map("error" -> message).asJava)

  private def errorResponse(status: Response.Status, message: String): Response =
    Response.status(status).entity(errorJson(message)).build()

  private def errorResponse(statusCode: Int, message: String): Response =
    Response.status(statusCode).entity(errorJson(message)).build()

  private[resource] def inferAudioContentType(path: NioPath): String = {
    val fileName = Option(path.getFileName).map(_.toString.toLowerCase).getOrElse("")
    if (fileName.endsWith(".mp3") || fileName.endsWith(".mpeg")) "audio/mpeg"
    else if (fileName.endsWith(".wav")) "audio/wav"
    else if (fileName.endsWith(".flac")) "audio/flac"
    else if (fileName.endsWith(".ogg") || fileName.endsWith(".oga")) "audio/ogg"
    else if (fileName.endsWith(".webm")) "audio/webm"
    else if (fileName.endsWith(".opus")) "audio/webm;codecs=opus"
    else if (fileName.endsWith(".amr")) "audio/amr"
    else if (fileName.endsWith(".m4a")) "audio/m4a"
    else "application/octet-stream"
  }

  /** Result of a paginated fetch — `truncated` is true if pagination stopped early. */
  private case class PageResult(
      models: java.util.List[java.util.Map[String, Object]],
      truncated: Boolean
  )
}
