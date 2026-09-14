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

/**
  * Pure validators for W3C Trace Context headers crossing the
  * Scala↔Python boundary. The single rule: if the inbound bytes do
  * not match the strict regex, we discard the value and start a fresh
  * trace. We never echo a rejected value back into a span, log, or
  * error message.
  *
  * Spec reference: https://www.w3.org/TR/trace-context/
  *
  * The regexes here intentionally do NOT use any context-sensitive
  * grouping or backreferences — keeps the validators safe against
  * pathological inputs (ReDoS) and trivially fast.
  */
object TraceparentValidator {

  /** W3C traceparent format: `<version>-<trace-id>-<parent-id>-<flags>`.
    *  We accept only version `00` (the only published version) with the
    *  canonical 32-hex / 16-hex / 2-hex layout. All hex lowercase per
    *  spec — uppercase is invalid.
    */
  private val TraceparentPattern =
    "^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$".r.pattern

  /** Bounded tracestate length. Spec recommends ≤512 chars. We are
    *  stricter to remove a small DoS surface — an attacker can't send
    *  a 1 MiB tracestate to balloon downstream context allocations.
    */
  val MaxTracestateLength: Int = 512

  /** Validate a traceparent header. Returns the input unchanged on
    *  success, None on any failure (rejected — caller starts a fresh
    *  trace). Null and empty are silent failures.
    */
  def validateTraceparent(header: String): Option[String] = {
    if (header == null || header.isEmpty) return None
    // Trace-id and parent-id must not be all-zero per spec — an
    // all-zero ID is a sentinel for "no value" and MUST be rejected.
    if (!TraceparentPattern.matcher(header).matches()) return None
    val parts = header.split('-')
    val traceId = parts(1)
    val spanId = parts(2)
    if (isAllZero(traceId) || isAllZero(spanId)) return None
    Some(header)
  }

  /** Validate a tracestate header. Spec: comma-separated list of
    *  key=value pairs, ASCII-printable only, total length capped.
    *  Returns the input unchanged on success, None on rejection.
    */
  def validateTracestate(header: String): Option[String] = {
    if (header == null || header.isEmpty) return None
    if (header.length > MaxTracestateLength) return None
    if (!isAsciiPrintable(header)) return None
    Some(header)
  }

  private def isAllZero(s: String): Boolean = {
    var i = 0
    while (i < s.length) {
      if (s.charAt(i) != '0') return false
      i += 1
    }
    true
  }

  private def isAsciiPrintable(s: String): Boolean = {
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      // Allow printable ASCII range (0x20-0x7E). Tab and CR/LF are
      // rejected — a tracestate must not span lines.
      if (c < 0x20 || c > 0x7e) return false
      i += 1
    }
    true
  }
}
