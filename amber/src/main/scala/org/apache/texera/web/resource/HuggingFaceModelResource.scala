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
import kong.unirest.Unirest

import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import java.nio.file.{Files, Path => NioPath, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._

/**
  * REST resource that proxies the Hugging Face Hub API to list
  * models for the HuggingFace operator.
  *
  * Browse mode:  GET /api/huggingface/models?task=text-generation
  *   Fetches ALL models for the task from HF Hub (paginated internally),
  *   caches the full list server-side, and returns it.
  *
  * Search mode:  GET /api/huggingface/models?task=text-generation&search=bert
  *   Forwards the search query to HF Hub API (searches all models).
  */
@Path("/huggingface")
@Produces(Array(MediaType.APPLICATION_JSON))
class HuggingFaceModelResource {

  import HuggingFaceModelResource._

  @GET
  @Path("/models")
  def listModels(
      @QueryParam("task") @DefaultValue("text-generation") task: String,
      @QueryParam("search") search: String
  ): Response = {
    try {
      val hfToken = Option(System.getenv("HF_TOKEN")).getOrElse("")

      // ── Search mode: forward query to HF Hub, return results directly ──
      if (search != null && search.trim.nonEmpty) {
        return fetchSearchResults(task, search.trim, hfToken)
      }

      // ── Browse mode: return ALL models for this task (cached) ──
      val cached = modelCache.get(task)
      if (cached != null) {
        return Response.ok(cached).build()
      }

      // Not cached — fetch all pages from HF Hub API
      val allModels = fetchAllModelsForTask(task, hfToken)
      val json = objectMapper.writeValueAsString(allModels)
      modelCache.put(task, json)

      Response.ok(json).build()

    } catch {
      case e: Exception =>
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"""{"error":"Failed to fetch models: ${e.getMessage}"}""")
          .build()
    }
  }

  @POST
  @Path("/upload-audio")
  @Consumes(Array(MediaType.WILDCARD))
  def uploadAudioReference(
      @QueryParam("filename") filename: String,
      bytes: Array[Byte]
  ): Response = {
    try {
      if (bytes == null || bytes.isEmpty) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity("""{"error":"Audio payload is empty."}""")
          .build()
      }

      val safeFileName = Option(filename)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(name => Paths.get(name).getFileName.toString)
        .getOrElse("audio.bin")
      val extension = {
        val idx = safeFileName.lastIndexOf('.')
        if (idx >= 0 && idx < safeFileName.length - 1) safeFileName.substring(idx) else ".bin"
      }

      val tempDir = Paths.get(System.getProperty("java.io.tmpdir"), "texera-hf-audio")
      Files.createDirectories(tempDir)
      val tempFile: NioPath = Files.createTempFile(tempDir, "hf-audio-", extension)
      Files.write(tempFile, bytes)

      val json = objectMapper.writeValueAsString(
        Map(
          "path" -> tempFile.toAbsolutePath.toString,
          "fileName" -> safeFileName
        ).asJava
      )
      Response.ok(json).build()
    } catch {
      case e: Exception =>
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"""{"error":"Failed to upload audio: ${e.getMessage}"}""")
          .build()
    }
  }

  @GET
  @Path("/audio-preview")
  def previewUploadedAudio(@QueryParam("path") path: String): Response = {
    try {
      val trimmedPath = Option(path).map(_.trim).getOrElse("")
      if (trimmedPath.isEmpty) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity("""{"error":"Audio path is required."}""")
          .build()
      }

      val tempDir = Paths
        .get(System.getProperty("java.io.tmpdir"), "texera-hf-audio")
        .toAbsolutePath
        .normalize()
      val requestedPath = Paths.get(trimmedPath).toAbsolutePath.normalize()
      if (!requestedPath.startsWith(tempDir)) {
        return Response
          .status(Response.Status.FORBIDDEN)
          .entity("""{"error":"Audio path is outside the allowed preview directory."}""")
          .build()
      }
      if (!Files.exists(requestedPath) || !Files.isRegularFile(requestedPath)) {
        return Response
          .status(Response.Status.NOT_FOUND)
          .entity("""{"error":"Uploaded audio file was not found."}""")
          .build()
      }

      val contentType = Option(Files.probeContentType(requestedPath))
        .filter(_.trim.nonEmpty)
        .getOrElse(inferAudioContentType(requestedPath))
      Response.ok(Files.readAllBytes(requestedPath), contentType).build()
    } catch {
      case e: Exception =>
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"""{"error":"Failed to read uploaded audio: ${e.getMessage}"}""")
          .build()
    }
  }

  @GET
  @Path("/media-proxy")
  def proxyRemoteMedia(@QueryParam("url") url: String): Response = {
    try {
      val trimmedUrl = Option(url).map(_.trim).getOrElse("")
      if (trimmedUrl.isEmpty) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity("""{"error":"Media URL is required."}""")
          .build()
      }
      if (!trimmedUrl.startsWith("http://") && !trimmedUrl.startsWith("https://")) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity("""{"error":"Only http(s) media URLs are supported."}""")
          .build()
      }

      val upstreamResponse = Unirest
        .get(trimmedUrl)
        .connectTimeout(10000)
        .socketTimeout(120000)
        .asBytes()

      if (upstreamResponse.getStatus != 200) {
        return Response
          .status(upstreamResponse.getStatus)
          .entity(
            s"""{"error":"Failed to fetch remote media: ${upstreamResponse.getStatusText}"}"""
          )
          .build()
      }

      val contentType = Option(upstreamResponse.getHeaders.getFirst("Content-Type"))
        .filter(_.trim.nonEmpty)
        .getOrElse(MediaType.APPLICATION_OCTET_STREAM)
      Response.ok(upstreamResponse.getBody, contentType).build()
    } catch {
      case e: Exception =>
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"""{"error":"Failed to proxy remote media: ${e.getMessage}"}""")
          .build()
    }
  }

  /** Search HF Hub for models matching a query within a task. */
  private def fetchSearchResults(task: String, query: String, hfToken: String): Response = {
    var request = Unirest
      .get("https://huggingface.co/api/models")
      .queryString("pipeline_tag", task)
      .queryString("sort", "downloads")
      .queryString("direction", "-1")
      .queryString("limit", "100")
      .queryString("filter", task)
      .queryString("inference", "warm")
      .queryString("search", query)

    if (hfToken.nonEmpty) {
      request = request.header("Authorization", s"Bearer $hfToken")
    }

    val hfResponse = request.asString()

    if (hfResponse.getStatus != 200) {
      return Response
        .status(hfResponse.getStatus)
        .entity(s"""{"error":"Hugging Face API error: ${hfResponse.getStatusText}"}""")
        .build()
    }

    val rawModels = objectMapper.readValue(hfResponse.getBody, listOfMapsType)
    val out = buildSimplifiedList(rawModels)
    Response.ok(objectMapper.writeValueAsString(out)).build()
  }

  /**
    * Fetch pipeline task tags from the Hugging Face Hub API.
    * GET /api/huggingface/tasks
    *
    * Returns a JSON array of objects: [{ "tag": "text-generation", "label": "Text Generation" }, ...]
    * The result is cached server-side for the lifetime of the process.
    */
  @GET
  @Path("/tasks")
  def listTasks(): Response = {
    try {
      val cached = taskCache.get("all")
      if (cached != null) {
        return Response.ok(cached).build()
      }

      val hfToken = Option(System.getenv("HF_TOKEN")).getOrElse("")
      var request = Unirest
        .get("https://huggingface.co/api/tasks")
        .connectTimeout(10000)
        .socketTimeout(15000)

      if (hfToken.nonEmpty) {
        request = request.header("Authorization", s"Bearer $hfToken")
      }

      val hfResponse = request.asString()

      if (hfResponse.getStatus != 200) {
        return Response
          .status(hfResponse.getStatus)
          .entity(s"""{"error":"Hugging Face API error: ${hfResponse.getStatusText}"}""")
          .build()
      }

      // /api/tasks returns a JSON object: { "<pipeline_tag>": { "label": "...", ... }, ... }
      // Using readTree so no entry is dropped regardless of its value type (null, array, etc.)
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

      // Filter out tasks that have no models available with hosted inference
      val availableTasks = taskList
        .parallelStream()
        .filter(task => hasModelsForTask(task.get("tag").toString, hfToken))
        .collect(Collectors.toList())

      val json = objectMapper.writeValueAsString(availableTasks)
      taskCache.put("all", json)
      Response.ok(json).build()

    } catch {
      case e: Exception =>
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"""{"error":"Failed to fetch tasks: ${e.getMessage}"}""")
          .build()
    }
  }

  /**
    * Fetch ALL models for a given task by paginating through the HF Hub API.
    * HF Hub uses a Link header with rel="next" for pagination.
    * We fetch pages of 1000 models at a time, up to MAX_PAGES pages.
    */
  private def fetchAllModelsForTask(
      task: String,
      hfToken: String
  ): java.util.List[java.util.Map[String, Object]] = {
    val allResults = new java.util.ArrayList[java.util.Map[String, Object]]()
    var nextUrl: String = null
    var pageCount = 0

    // First request — inference=warm limits to models with hosted Inference API
    var request = Unirest
      .get("https://huggingface.co/api/models")
      .queryString("pipeline_tag", task)
      .queryString("sort", "downloads")
      .queryString("direction", "-1")
      .queryString("limit", PAGE_SIZE.toString)
      .queryString("filter", task)
      .queryString("inference", "warm")
      .connectTimeout(10000)
      .socketTimeout(30000)

    if (hfToken.nonEmpty) {
      request = request.header("Authorization", s"Bearer $hfToken")
    }

    var hfResponse = request.asString()

    if (hfResponse.getStatus != 200) {
      throw new RuntimeException(
        s"HF API returned ${hfResponse.getStatus}: ${hfResponse.getStatusText}"
      )
    }

    var rawModels = objectMapper.readValue(hfResponse.getBody, listOfMapsType)
    allResults.addAll(buildSimplifiedList(rawModels))
    pageCount += 1

    // Extract next page URL from Link header
    nextUrl = extractNextLink(hfResponse.getHeaders.getFirst("Link"))

    // Fetch remaining pages until exhausted
    while (nextUrl != null) {
      var nextRequest = Unirest
        .get(nextUrl)
        .connectTimeout(10000)
        .socketTimeout(30000)
      if (hfToken.nonEmpty) {
        nextRequest = nextRequest.header("Authorization", s"Bearer $hfToken")
      }

      hfResponse = nextRequest.asString()

      if (hfResponse.getStatus != 200) {
        // Stop paginating on error, return what we have so far
        return allResults
      }

      rawModels = objectMapper.readValue(hfResponse.getBody, listOfMapsType)
      allResults.addAll(buildSimplifiedList(rawModels))
      pageCount += 1

      nextUrl = extractNextLink(hfResponse.getHeaders.getFirst("Link"))
    }

    allResults
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
    * Uses a limit=1 query to avoid fetching unnecessary data.
    */
  private def hasModelsForTask(task: String, hfToken: String): Boolean = {
    try {
      var request = Unirest
        .get("https://huggingface.co/api/models")
        .queryString("pipeline_tag", task)
        .queryString("filter", task)
        .queryString("limit", "1")
        .queryString("inference", "warm")
        .connectTimeout(5000)
        .socketTimeout(10000)

      if (hfToken.nonEmpty) {
        request = request.header("Authorization", s"Bearer $hfToken")
      }

      val response = request.asString()
      if (response.getStatus != 200) return false

      val models = objectMapper.readValue(response.getBody, listOfMapsType)
      !models.isEmpty
    } catch {
      case _: Exception => false
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
  private val objectMapper: ObjectMapper = new ObjectMapper()

  private val listOfMapsType =
    new TypeReference[java.util.List[java.util.Map[String, Object]]]() {}

  /** Server-side cache: task → JSON string of all models. Thread-safe. */
  private val modelCache = new ConcurrentHashMap[String, String]()

  /** Server-side cache: "all" → JSON string of all pipeline tags. Thread-safe. */
  private val taskCache = new ConcurrentHashMap[String, String]()

  private def inferAudioContentType(path: NioPath): String = {
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

  /** Number of models to fetch per HF API page. */
  private val PAGE_SIZE = 1000

}
