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

package org.apache.texera.amber.operator.aiagent

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{HttpURLConnection, URL}

/**
  * Read a PDF from a public HTTP(S) URL and return its text content
  * for an LLM to consume.
  *
  * Self-contained on the JVM via Apache PDFBox 2.x — no external service.
  * Supports optional page-range filtering (1-based inclusive) and a hard
  * character cap with truncation marker, since real-world PDFs are routinely
  * larger than the model's context window.
  */
object PdfReader {
  final val DefaultMaxChars = 100000
  final val DefaultMaxBytes = 32 * 1024 * 1024
  final val DefaultTimeoutMs = 30000
  final val TruncationMarker = "\n\n[... truncated ...]"
  private final val UserAgent =
    "Mozilla/5.0 (compatible; TexeraAIAgent/1.0; +https://texera.io)"

  def readText(
      source: String,
      startPage: Option[Int] = None,
      endPage: Option[Int] = None,
      maxChars: Int = DefaultMaxChars,
      maxBytes: Int = DefaultMaxBytes,
      timeoutMs: Int = DefaultTimeoutMs
  ): String = {
    require(source != null && source.trim.nonEmpty, "source is required")
    val bytes = loadBytes(source.trim, maxBytes, timeoutMs)
    val effectiveMax = if (maxChars <= 0) DefaultMaxChars else maxChars
    val document = PDDocument.load(bytes)
    try {
      val stripper = new PDFTextStripper()
      val totalPages = document.getNumberOfPages
      stripper.setStartPage(startPage.map(_.max(1)).getOrElse(1))
      stripper.setEndPage(endPage.map(_.min(totalPages)).getOrElse(totalPages))
      val text = Option(stripper.getText(document)).getOrElse("").trim
      truncate(text, effectiveMax)
    } finally {
      document.close()
    }
  }

  private def loadBytes(source: String, maxBytes: Int, timeoutMs: Int): Array[Byte] = {
    val uri = AIAgentUrlSafety.validatePublicHttpUrl(source)
    downloadBytes(uri.toString, maxBytes, timeoutMs, redirectCount = 0)
  }

  private def downloadBytes(
      url: String,
      maxBytes: Int,
      timeoutMs: Int,
      redirectCount: Int
  ): Array[Byte] = {
    require(redirectCount <= 5, s"Too many redirects fetching $url")
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestProperty("User-Agent", UserAgent)
    connection.setConnectTimeout(timeoutMs)
    connection.setReadTimeout(timeoutMs)
    connection.setInstanceFollowRedirects(false)
    val responseCode = connection.getResponseCode
    if (responseCode >= 300 && responseCode < 400) {
      val location = connection.getHeaderField("Location")
      require(location != null && location.trim.nonEmpty, s"Redirect missing Location fetching $url")
      val redirected = new URL(new URL(url), location.trim).toString
      AIAgentUrlSafety.validatePublicHttpUrl(redirected)
      return downloadBytes(redirected, maxBytes, timeoutMs, redirectCount + 1)
    }
    require(responseCode >= 200 && responseCode < 300, s"HTTP $responseCode fetching $url")
    val stream: InputStream = connection.getInputStream
    try readWithCap(stream, maxBytes)
    finally stream.close()
  }

  private def readWithCap(stream: InputStream, maxBytes: Int): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val chunk = new Array[Byte](8192)
    var total = 0
    var read = stream.read(chunk)
    while (read != -1) {
      total += read
      require(total <= maxBytes, s"PDF body exceeds maxBytes ($maxBytes)")
      buffer.write(chunk, 0, read)
      read = stream.read(chunk)
    }
    buffer.toByteArray
  }

  private[aiagent] def truncate(text: String, maxChars: Int): String =
    if (text.length <= maxChars) text
    else text.substring(0, maxChars).stripTrailing() + TruncationMarker
}
