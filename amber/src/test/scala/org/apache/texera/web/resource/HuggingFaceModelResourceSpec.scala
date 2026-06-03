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

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import javax.ws.rs.core.Response

/**
  * Tests for [[HuggingFaceModelResource]] covering the validation, security,
  * caching, and filesystem behavior that can be exercised without contacting
  * Hugging Face Hub. Paths that require live HF API calls (the actual fetch
  * loops in `listModels` browse-mode-uncached and `listTasks` uncached) are
  * left to integration testing.
  */
class HuggingFaceModelResourceSpec extends AnyFunSuite with BeforeAndAfterEach {

  import HuggingFaceModelResource._

  private val mapper = new ObjectMapper()
  private var resource: HuggingFaceModelResource = _

  override def beforeEach(): Unit = {
    resource = new HuggingFaceModelResource()
    // Reset caches between tests so cache hits from one test can't leak into another.
    modelCache.invalidateAll()
    taskCache.invalidateAll()
    // Make sure the audio temp dir exists for tests that read from it.
    Files.createDirectories(audioTempDir)
  }

  override def afterEach(): Unit = {
    // Clean up any temp files this test created.
    if (Files.exists(audioTempDir)) {
      val stream = Files.list(audioTempDir)
      try {
        stream.forEach { p =>
          try Files.deleteIfExists(p)
          catch { case _: Exception => () }
        }
      } finally {
        stream.close()
      }
    }
    modelCache.invalidateAll()
    taskCache.invalidateAll()
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

  // Helper: build a small in-memory InputStream from a UTF-8 string.
  private def streamOf(s: String): InputStream =
    new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8))

  // Helper: build an InputStream of `n` zero-bytes.
  private def streamOfBytes(n: Int): InputStream =
    new ByteArrayInputStream(new Array[Byte](n))

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
  // inferAudioContentType — extension → MIME type
  // ────────────────────────────────────────────────────────────────────────

  test("inferAudioContentType returns audio/mpeg for .mp3") {
    assert(inferAudioContentType(Paths.get("clip.mp3")) == "audio/mpeg")
  }

  test("inferAudioContentType returns audio/mpeg for .mpeg") {
    assert(inferAudioContentType(Paths.get("clip.mpeg")) == "audio/mpeg")
  }

  test("inferAudioContentType returns audio/wav for .wav") {
    assert(inferAudioContentType(Paths.get("clip.wav")) == "audio/wav")
  }

  test("inferAudioContentType returns audio/flac for .flac") {
    assert(inferAudioContentType(Paths.get("clip.flac")) == "audio/flac")
  }

  test("inferAudioContentType returns audio/ogg for .ogg") {
    assert(inferAudioContentType(Paths.get("clip.ogg")) == "audio/ogg")
  }

  test("inferAudioContentType returns audio/ogg for .oga") {
    assert(inferAudioContentType(Paths.get("clip.oga")) == "audio/ogg")
  }

  test("inferAudioContentType returns audio/webm for .webm") {
    assert(inferAudioContentType(Paths.get("clip.webm")) == "audio/webm")
  }

  test("inferAudioContentType returns audio/webm;codecs=opus for .opus") {
    assert(inferAudioContentType(Paths.get("clip.opus")) == "audio/webm;codecs=opus")
  }

  test("inferAudioContentType returns audio/amr for .amr") {
    assert(inferAudioContentType(Paths.get("clip.amr")) == "audio/amr")
  }

  test("inferAudioContentType returns audio/m4a for .m4a") {
    assert(inferAudioContentType(Paths.get("clip.m4a")) == "audio/m4a")
  }

  test("inferAudioContentType falls back to octet-stream for unknown extension") {
    assert(inferAudioContentType(Paths.get("clip.xyz")) == "application/octet-stream")
    assert(inferAudioContentType(Paths.get("noextension")) == "application/octet-stream")
  }

  test("inferAudioContentType is case-insensitive") {
    assert(inferAudioContentType(Paths.get("clip.WAV")) == "audio/wav")
    assert(inferAudioContentType(Paths.get("clip.MP3")) == "audio/mpeg")
  }

  // ────────────────────────────────────────────────────────────────────────
  // uploadAudioReference — input validation & size cap
  // ────────────────────────────────────────────────────────────────────────

  test("uploadAudioReference returns 400 when stream is null") {
    val response = resource.uploadAudioReference("voice.wav", null)
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference returns 400 when stream is empty") {
    val response = resource.uploadAudioReference("voice.wav", streamOfBytes(0))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference rejects .sh extension") {
    val response = resource.uploadAudioReference("evil.sh", streamOf("payload"))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference rejects .html extension") {
    val response = resource.uploadAudioReference("trick.html", streamOf("<script>"))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference rejects .bat extension") {
    val response = resource.uploadAudioReference("run.bat", streamOf("@echo off"))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference rejects .exe extension") {
    val response = resource.uploadAudioReference("malware.exe", streamOf("MZ"))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference rejects files with no extension") {
    val response = resource.uploadAudioReference("recording", streamOf("data"))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference rejects null filename (default audio.bin not in allowlist)") {
    val response = resource.uploadAudioReference(null, streamOf("data"))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference rejects empty filename (default audio.bin not in allowlist)") {
    val response = resource.uploadAudioReference("", streamOf("data"))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference rejects whitespace-only filename") {
    val response = resource.uploadAudioReference("   ", streamOf("data"))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference accepts a valid .wav upload") {
    val payload = "RIFF....WAVE....fake-wav-content".getBytes(StandardCharsets.UTF_8)
    val response = resource.uploadAudioReference("voice.wav", new ByteArrayInputStream(payload))
    assert(response.getStatus == 200)

    val node = mapper.readTree(entityString(response))
    assert(node.has("path"))
    assert(node.has("fileName"))
    assert(node.get("fileName").asText() == "voice.wav")

    // Verify the file was actually written with the right contents.
    val savedPath = Paths.get(node.get("path").asText())
    assert(Files.exists(savedPath))
    assert(Files.readAllBytes(savedPath).sameElements(payload))
    // The saved file should land inside the audioTempDir.
    assert(savedPath.toAbsolutePath.normalize().startsWith(audioTempDir.toAbsolutePath.normalize()))
  }

  test("uploadAudioReference lowercases the extension for the temp file") {
    val response = resource.uploadAudioReference("voice.WAV", streamOf("RIFF"))
    assert(response.getStatus == 200)

    val node = mapper.readTree(entityString(response))
    val savedPath = Paths.get(node.get("path").asText())
    assert(savedPath.getFileName.toString.endsWith(".wav"))
  }

  test("uploadAudioReference strips path components from filename") {
    // ?filename=../../etc/passwd should be reduced to passwd (no extension) — rejected
    val response = resource.uploadAudioReference("../../etc/passwd", streamOf("data"))
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("uploadAudioReference returns 413 for payload exceeding MAX_AUDIO_BYTES") {
    val oversize = MAX_AUDIO_BYTES.toInt + 1
    val response = resource.uploadAudioReference("big.wav", streamOfBytes(oversize))
    assert(response.getStatus == 413)
    assertErrorBody(response)
  }

  test("uploadAudioReference cleans up partial file when size cap is exceeded") {
    val sweepBefore = listAudioTempFiles()
    val oversize = MAX_AUDIO_BYTES.toInt + 1
    val response = resource.uploadAudioReference("big.wav", streamOfBytes(oversize))
    assert(response.getStatus == 413)
    val sweepAfter = listAudioTempFiles()
    // No new file should remain after the rejection (existing files unchanged).
    assert(
      sweepAfter.length <= sweepBefore.length,
      s"oversize upload left a partial file: before=$sweepBefore after=$sweepAfter"
    )
  }

  test("uploadAudioReference accepts all allowlisted extensions") {
    ALLOWED_AUDIO_EXTENSIONS.foreach { ext =>
      val response = resource.uploadAudioReference(s"clip$ext", streamOf("data"))
      assert(response.getStatus == 200, s"extension $ext should have been accepted")
    }
  }

  private def listAudioTempFiles(): Array[Path] = {
    if (!Files.exists(audioTempDir)) return Array.empty
    val stream = Files.list(audioTempDir)
    try {
      val arr = stream.toArray.asInstanceOf[Array[Object]].map(_.asInstanceOf[Path])
      arr
    } finally {
      stream.close()
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // previewUploadedAudio — path validation
  // ────────────────────────────────────────────────────────────────────────

  test("previewUploadedAudio returns 400 when path is null") {
    val response = resource.previewUploadedAudio(null)
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("previewUploadedAudio returns 400 when path is empty") {
    val response = resource.previewUploadedAudio("")
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("previewUploadedAudio returns 400 when path is whitespace") {
    val response = resource.previewUploadedAudio("   ")
    assert(response.getStatus == 400)
    assertErrorBody(response)
  }

  test("previewUploadedAudio returns 403 when path is outside the temp directory") {
    val response = resource.previewUploadedAudio("/etc/passwd")
    assert(response.getStatus == 403)
    assertErrorBody(response)
  }

  test("previewUploadedAudio rejects path traversal attempts") {
    val traversalPath =
      audioTempDir.toAbsolutePath.toString + "/../../etc/passwd"
    val response = resource.previewUploadedAudio(traversalPath)
    assert(response.getStatus == 403)
    assertErrorBody(response)
  }

  test("previewUploadedAudio returns 404 for a non-existent file inside temp dir") {
    val ghost = audioTempDir.resolve("does-not-exist.wav").toAbsolutePath.toString
    val response = resource.previewUploadedAudio(ghost)
    assert(response.getStatus == 404)
    assertErrorBody(response)
  }

  test("previewUploadedAudio returns 404 when path points to a directory, not a file") {
    val response = resource.previewUploadedAudio(audioTempDir.toAbsolutePath.toString)
    assert(response.getStatus == 404)
    assertErrorBody(response)
  }

  test("previewUploadedAudio streams back a valid file with correct content-type") {
    val payload = "fake-wav-bytes".getBytes(StandardCharsets.UTF_8)
    val file = Files.createTempFile(audioTempDir, "test-preview-", ".wav")
    Files.write(file, payload)

    val response = resource.previewUploadedAudio(file.toAbsolutePath.toString)
    assert(response.getStatus == 200)
    val bytes = entityBytes(response)
    assert(bytes.sameElements(payload))
  }

  test(
    "previewUploadedAudio returns 413 when the on-disk file exceeds MAX_AUDIO_BYTES (defense-in-depth)"
  ) {
    // /upload-audio caps ingest at MAX_AUDIO_BYTES, but the preview endpoint
    // shouldn't trust that invariant — a future bug or out-of-band write could
    // leave an oversized file in the temp dir. Reads of those files must not
    // OOM the JVM.
    val file = Files.createTempFile(audioTempDir, "test-oversize-", ".wav")
    // Create a sparse file of size MAX_AUDIO_BYTES + 1 without actually
    // writing that many bytes to disk.
    val raf = new java.io.RandomAccessFile(file.toFile, "rw")
    try raf.setLength(MAX_AUDIO_BYTES + 1)
    finally raf.close()

    val response = resource.previewUploadedAudio(file.toAbsolutePath.toString)
    assert(response.getStatus == 413)
    assertErrorBody(response)
  }

  test("previewUploadedAudio normalizes the path before checking containment") {
    val payload = "ok".getBytes(StandardCharsets.UTF_8)
    val file = Files.createTempFile(audioTempDir, "test-norm-", ".wav")
    Files.write(file, payload)

    // Same file referenced via a non-normalized path (extra slashes / dot-segments).
    val weird = audioTempDir.toAbsolutePath.toString + "/./" + file.getFileName.toString
    val response = resource.previewUploadedAudio(weird)
    assert(response.getStatus == 200)
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

  // ────────────────────────────────────────────────────────────────────────
  // sweepOldAudioFiles — temp directory cleanup
  // ────────────────────────────────────────────────────────────────────────

  test("sweepOldAudioFiles deletes files older than the TTL") {
    val oldFile = Files.createTempFile(audioTempDir, "test-sweep-old-", ".wav")
    Files.write(oldFile, "old".getBytes(StandardCharsets.UTF_8))
    // Force the lastModified time to be older than the TTL window.
    val oldTime = java.nio.file.attribute.FileTime.fromMillis(
      System.currentTimeMillis() - AUDIO_TEMP_TTL_MS - 60000L
    )
    Files.setLastModifiedTime(oldFile, oldTime)

    sweepOldAudioFiles(audioTempDir)

    assert(!Files.exists(oldFile), "old file should have been swept")
  }

  test("sweepOldAudioFiles preserves files newer than the TTL") {
    val freshFile = Files.createTempFile(audioTempDir, "test-sweep-fresh-", ".wav")
    Files.write(freshFile, "fresh".getBytes(StandardCharsets.UTF_8))
    // Default mtime is now; explicitly set to be safe.
    val recentTime = java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis())
    Files.setLastModifiedTime(freshFile, recentTime)

    sweepOldAudioFiles(audioTempDir)

    assert(Files.exists(freshFile), "fresh file should have been preserved")
  }

  test("sweepOldAudioFiles handles a missing directory gracefully") {
    val ghostDir =
      Paths.get(System.getProperty("java.io.tmpdir"), "texera-hf-audio-ghost-" + System.nanoTime())
    // Don't create it. The sweep should swallow the IOException and not throw.
    sweepOldAudioFiles(ghostDir)
    // (no assertion needed — reaching this line means no exception escaped)
    succeed
  }

  test("sweepOldAudioFiles only deletes regular files, not subdirectories") {
    val subdir = Files.createTempDirectory(audioTempDir, "test-sweep-subdir-")
    val oldTime = java.nio.file.attribute.FileTime.fromMillis(
      System.currentTimeMillis() - AUDIO_TEMP_TTL_MS - 60000L
    )
    Files.setLastModifiedTime(subdir, oldTime)

    sweepOldAudioFiles(audioTempDir)

    assert(Files.exists(subdir), "subdirectory should be preserved (sweep only deletes files)")
    // cleanup
    Files.deleteIfExists(subdir)
  }
}
