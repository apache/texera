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
  * Pure functions that sanitize log records before they leave the
  * process via the OTel logs bridge. Lives in its own object so the
  * security-critical behaviour can be unit-tested without a Logback
  * fixture.
  *
  * Three invariants:
  *  1. No control characters in the body (prevents log forging via
  *     CR/LF injection in user-supplied strings).
  *  2. No oversized bodies (a 1 GiB log line must never reach the
  *     exporter).
  *  3. No secrets in plain text (Bearer tokens, password=, AWS keys).
  *
  * Plus an MDC allowlist so accidental MDC pollution from a downstream
  * library cannot leak unintended fields into the exporter.
  */
object LogSanitizer {

  /** Per-record body cap. The OTel SDK and OTLP have higher limits,
    *  but 16 KiB is plenty for a useful log line and protects the
    *  collector from a runaway log.
    */
  val MaxBodyBytes: Int = 16 * 1024

  /** Suffix appended to truncated bodies. Chosen to be visually
    *  obvious in a UI but short enough not to dominate the cap.
    */
  val TruncatedMarker: String = "...[truncated]"

  /** C0 control characters except TAB (0x09). Stripping CR/LF here
    *  prevents log forging via newline injection in user-supplied
    *  message bodies. DEL (0x7F) included for the same reason.
    */
  private val C0ControlRegex = "[\\x00-\\x08\\x0A-\\x1F\\x7F]".r

  /** Secret patterns. Order is significant: most specific first so a
    *  partial match doesn't shadow a tighter pattern.
    */
  private val SecretPatterns: Seq[scala.util.matching.Regex] = Seq(
    // Bearer token in header form.
    """(?i)Bearer\s+[A-Za-z0-9._\-/+=]{8,}""".r,
    // Bare JWT (header.payload.signature).
    """eyJ[A-Za-z0-9_\-]{6,}\.eyJ[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{6,}""".r,
    // AWS access key ID.
    """AKIA[0-9A-Z]{16}""".r,
    // Labelled AWS secret access key.
    """(?i)aws_secret_access_key\s*[=:]\s*[A-Za-z0-9/+=]{20,}""".r,
    // Labelled credential in key=val, JSON, or quoted form.
    ("""(?i)(?:password|passwd|pwd|secret|token|api[_-]?key|client[_-]?secret|""" +
      """access[_-]?token)"?\s*[=:]\s*"?[^\s,;"']+""").r
  )

  /** MDC keys we will forward to OTel log attributes. Anything else
    *  is dropped — additions require a code change + reviewer
    *  acknowledgement of the privacy implications.
    */
  val AllowedMdcKeys: Set[String] = Set(
    "trace_id",
    "span_id",
    "texera.user.id",
    "texera.workflow.id",
    "texera.execution.id",
    // Computing-unit id identifies the dev process / k8s pod that
    // emitted the record. Required for the dashboard's CU-scoped
    // log filter — without this key in the allowlist, the OTel
    // appender silently strips it and the CU filter matches nothing.
    "texera.computing_unit.id",
    "texera.project.id",
    "texera.operator.id"
  )

  /** Apply all three body-side transformations. Idempotent — running
    *  sanitize on already-sanitized output is a no-op.
    */
  def sanitize(body: String): String = {
    if (body == null || body.isEmpty) return ""
    val stripped = C0ControlRegex.replaceAllIn(body, "")
    val scrubbed = SecretPatterns.foldLeft(stripped) { (acc, p) =>
      p.replaceAllIn(acc, "[REDACTED]")
    }
    truncate(scrubbed)
  }

  /** Truncate to MaxBodyBytes, appending the marker if cut. Public so
    *  callers building a body outside `sanitize` can enforce the cap.
    */
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
