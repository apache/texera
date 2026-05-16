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

import com.typesafe.scalalogging.LazyLogging
import com.vladsch.flexmark.html2md.converter.FlexmarkHtmlConverter
import net.dankito.readability4j.Readability4J
import org.jsoup.Jsoup

/**
  * Fetch a URL and return a token-efficient Markdown representation of the
  * main content, suitable to hand to an LLM.
  *
  * Pipeline:
  *   1. HTTP GET via jsoup (timeout + size cap, browser-like User-Agent)
  *   2. Readability4J to strip nav/footer/sidebar/ads and keep the article body
  *   3. flexmark-html2md to convert the cleaned HTML to Markdown
  *   4. Truncate to `maxChars` with a clear marker
  *
  * Self-contained on the JVM — no third-party service, no rate limits.
  */
object UrlFetcher extends LazyLogging {
  final val DefaultMaxChars = 50000
  final val DefaultTimeoutMs = 15000
  final val DefaultMaxBodyBytes = 5 * 1024 * 1024
  final val TruncationMarker = "\n\n[... truncated ...]"
  private final val MaxRedirects = 5
  private final val UserAgent =
    "Mozilla/5.0 (compatible; TexeraAIAgent/1.0; +https://texera.io)"

  private lazy val htmlToMd: FlexmarkHtmlConverter =
    FlexmarkHtmlConverter.builder().build()

  def fetchAsMarkdown(
      url: String,
      maxChars: Int = DefaultMaxChars,
      timeoutMs: Int = DefaultTimeoutMs,
      maxBodyBytes: Int = DefaultMaxBodyBytes
  ): String = {
    require(url != null && url.trim.nonEmpty, "url is required")
    val target = ensureScheme(url.trim)
    val effectiveMax = if (maxChars <= 0) DefaultMaxChars else maxChars
    fetchAsMarkdownChecked(target, effectiveMax, timeoutMs, maxBodyBytes, 0)
  }

  private def fetchAsMarkdownChecked(
      target: String,
      maxChars: Int,
      timeoutMs: Int,
      maxBodyBytes: Int,
      redirectCount: Int
  ): String = {
    val targetUri = AIAgentUrlSafety.validatePublicHttpUrl(target)
    val fetchStart = System.currentTimeMillis()
    val response = Jsoup
      .connect(targetUri.toString)
      .userAgent(UserAgent)
      .timeout(timeoutMs)
      .maxBodySize(maxBodyBytes)
      .followRedirects(false)
      .ignoreContentType(false)
      .ignoreHttpErrors(true)
      .execute()
    val statusCode = response.statusCode()
    if (statusCode >= 300 && statusCode < 400) {
      require(redirectCount < MaxRedirects, s"Too many redirects fetching $target")
      val location = response.header("Location")
      require(location != null && location.trim.nonEmpty, s"Redirect missing Location fetching $target")
      val redirected = targetUri.resolve(location.trim).toString
      return fetchAsMarkdownChecked(redirected, maxChars, timeoutMs, maxBodyBytes, redirectCount + 1)
    }
    require(statusCode >= 200 && statusCode < 300, s"HTTP $statusCode fetching $target")
    val html = response.body()
    val fetchMs = System.currentTimeMillis() - fetchStart

    val readability = new Readability4J(targetUri.toString, html)
    val article = readability.parse()
    val articleHtml = Option(article.getArticleContent)
      .map(_.outerHtml())
      .filter(_.nonEmpty)
      .getOrElse(html)

    val markdown = htmlToMd.convert(articleHtml).trim
    val out = truncate(markdown, maxChars)
    logger.info(
      s"[UrlFetcher] url=$targetUri fetchMs=$fetchMs htmlLen=${html.length} mdLen=${out.length}"
    )
    out
  }

  private[aiagent] def truncate(text: String, maxChars: Int): String =
    if (text.length <= maxChars) text
    else text.substring(0, maxChars).stripTrailing() + TruncationMarker

  private def ensureScheme(url: String): String =
    if (url.startsWith("http://") || url.startsWith("https://")) url
    else "https://" + url
}
