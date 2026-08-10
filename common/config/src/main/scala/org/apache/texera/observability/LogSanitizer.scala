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
  * filter MDC to an allowlist.
  */
object LogSanitizer {

  /** Per-record body byte cap. */
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

  /** MDC keys forwarded to OTel log attributes; others are dropped. */
  val AllowedMdcKeys: Set[String] = Set(
    "trace_id",
    "span_id",
    "texera.user.id",
    "texera.workflow.id",
    "texera.execution.id",
    "texera.computing_unit.id",
    "texera.project.id",
    "texera.operator.id"
  )

  /** Strip control chars, redact secrets, then truncate. Idempotent. */
  def sanitize(body: String): String = {
    if (body == null || body.isEmpty) return ""
    val stripped = C0ControlRegex.replaceAllIn(body, "")
    val scrubbed = SecretPatterns.foldLeft(stripped) { (acc, p) =>
      p.replaceAllIn(acc, "[REDACTED]")
    }
    truncate(scrubbed)
  }

  /** Truncate to MaxBodyBytes, appending the marker if cut. */
  def truncate(body: String): String = {
    if (body.length <= MaxBodyBytes) body
    else body.substring(0, MaxBodyBytes - TruncatedMarker.length) + TruncatedMarker
  }

  /** Filter an MDC map to the allowlist. Null-safe. */
  def filterMdc(mdc: java.util.Map[String, String]): Map[String, String] = {
    if (mdc == null) return Map.empty
    mdc.asScala.iterator.collect {
      case (k, v) if k != null && AllowedMdcKeys.contains(k) && v != null => k -> v
    }.toMap
  }
}
