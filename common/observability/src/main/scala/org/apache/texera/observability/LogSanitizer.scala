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

package org.apache.texera.observability

import scala.jdk.CollectionConverters._

/**
  * Pure functions that sanitize log bodies and MDC before export:
  * strip control characters, redact secrets, cap body size, and
  * filter MDC down by dropping denied keys.
  */
object LogSanitizer {

  /** Per-record body length cap, in chars. */
  val MaxBodyBytes: Int = 16 * 1024

  /** Suffix appended to truncated bodies. */
  val TruncatedMarker: String = "...[truncated]"

  /** C0 control characters except TAB (0x09), plus DEL (0x7F). */
  private val C0ControlRegex = "[\\x00-\\x08\\x0A-\\x1F\\x7F]".r

  /** Secret patterns, redacted from bodies. Most specific first. */
  private val SecretPatterns: Seq[scala.util.matching.Regex] = Seq(
    // Bearer token
    """(?i)Bearer\s+[A-Za-z0-9._\-/+=]{8,}""".r,
    // password=... or password: ...
    """(?i)password\s*[=:]\s*[^\s,;"']+""".r,
    // AWS access key ID
    """AKIA[0-9A-Z]{16}""".r,
    // labelled AWS secret access key
    """(?i)aws_secret_access_key\s*[=:]\s*[A-Za-z0-9/+=]{20,}""".r
  )

  /** MDC keys never forwarded to OTel log attributes. Default-allow: any key
    *  our instrumentation sets is exported, so adding a new correlation field
    *  needs no edit here. Only the noisy keys Pekko's SLF4J bridge injects are
    *  dropped, since they are redundant with the log body and would bloat every
    *  exported record. Values that pass through are still run through
    *  [[sanitize]], so secret-shaped content is redacted regardless of key; a
    *  key whose name looks credential-bearing (see [[isSecretKey]]) has its
    *  value redacted wholesale.
    */
  val DeniedMdcKeys: Set[String] = Set(
    "sourceThread",
    "pekkoSource",
    "pekkoAddress",
    "pekkoTimestamp",
    "sourceActorSystem"
  )

  /** Substrings marking an MDC key as credential-bearing. A matching key has
    *  its value redacted whole, since the value alone (e.g. a bare password)
    *  need not match any [[SecretPatterns]] regex to be a secret.
    */
  private val SecretKeySubstrings: Seq[String] =
    Seq(
      "password",
      "passwd",
      "pwd",
      "secret",
      "token",
      "apikey",
      "api_key",
      "authorization",
      "credential"
    )

  private def isSecretKey(key: String): Boolean = {
    val k = key.toLowerCase
    SecretKeySubstrings.exists(k.contains)
  }

  /** Strip C0 control characters (except TAB) and DEL. Null-safe. */
  def stripControlChars(body: String): String =
    if (body == null) "" else C0ControlRegex.replaceAllIn(body, "")

  /** Redact secret-shaped substrings. Null-safe, idempotent. */
  def redactSecrets(body: String): String =
    if (body == null) ""
    else SecretPatterns.foldLeft(body)((acc, p) => p.replaceAllIn(acc, "[REDACTED]"))

  /** Strip control chars, redact secrets, then truncate. Idempotent. */
  def sanitize(body: String): String = {
    if (body == null || body.isEmpty) return ""
    truncate(redactSecrets(stripControlChars(body)))
  }

  /** Truncate to MaxBodyBytes, appending the marker if cut. */
  def truncate(body: String): String = {
    if (body.length <= MaxBodyBytes) body
    else body.substring(0, MaxBodyBytes - TruncatedMarker.length) + TruncatedMarker
  }

  /** Drop denied MDC keys, sanitize the surviving values. A key whose name is
    *  credential-bearing has its value redacted whole. Null-safe.
    */
  def filterMdc(mdc: java.util.Map[String, String]): Map[String, String] = {
    if (mdc == null) return Map.empty
    mdc.asScala.iterator.collect {
      case (k, v) if k != null && v != null && !DeniedMdcKeys.contains(k) =>
        if (isSecretKey(k)) k -> "[REDACTED]" else k -> sanitize(v)
    }.toMap
  }
}
