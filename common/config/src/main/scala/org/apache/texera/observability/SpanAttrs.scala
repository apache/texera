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

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{Span, SpanBuilder}

/**
 * Thin helper for setting span attributes safely.
 *
 * Three rules:
 *  1. Typed setters only — no public escape hatch for arbitrary
 *     untyped strings to land on a span as untrusted free text.
 *  2. Free-text values are CRLF-stripped + capped at
 *     [[FreeTextMaxLen]] to prevent log/span forging via embedded
 *     newlines.
 *  3. Operator IDs and workflow/execution IDs must match a strict
 *     character set — otherwise dropped silently (the operator
 *     identifier should be a stable internal value, not user free
 *     text).
 */
object SpanAttrs {

  /** Maximum length for free-text span attribute values. */
  val FreeTextMaxLen: Int = 256

  /** Validates the shape we accept for operator IDs: alnum + `_.-`,
   *  1–64 chars. Anything else is dropped (not coerced — we'd rather
   *  miss a label than leak an unbounded string into a span). */
  private val OperatorIdPattern = "^[A-Za-z0-9_.\\-]{1,64}$".r.pattern

  // ---- Standard Texera correlation labels ------------------------------

  val WorkflowId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.workflow.id")
  val ExecutionId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.execution.id")
  val ProjectId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.project.id")
  val UserId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.user.id")
  val OperatorId: AttributeKey[String] = AttributeKey.stringKey("texera.operator.id")
  val OperatorName: AttributeKey[String] = AttributeKey.stringKey("texera.operator.name")
  val Outcome: AttributeKey[String] = AttributeKey.stringKey("texera.outcome")

  // ---- Typed setters for SpanBuilder (used at span-start time) ---------

  def withWorkflowId(b: SpanBuilder, id: Long): SpanBuilder =
    b.setAttribute(WorkflowId, java.lang.Long.valueOf(id))

  def withExecutionId(b: SpanBuilder, id: Long): SpanBuilder =
    b.setAttribute(ExecutionId, java.lang.Long.valueOf(id))

  def withProjectId(b: SpanBuilder, id: Long): SpanBuilder =
    b.setAttribute(ProjectId, java.lang.Long.valueOf(id))

  def withUserId(b: SpanBuilder, id: Long): SpanBuilder =
    b.setAttribute(UserId, java.lang.Long.valueOf(id))

  /** Sets the operator id only if it passes the strict character
   *  check; otherwise the attribute is omitted. Returns the same
   *  builder either way for fluent chaining. */
  def withOperatorId(b: SpanBuilder, id: String): SpanBuilder = {
    if (id != null && OperatorIdPattern.matcher(id).matches()) {
      b.setAttribute(OperatorId, id)
    }
    b
  }

  /** Sets a free-text label after stripping CRLF and capping length. */
  def withOperatorName(b: SpanBuilder, name: String): SpanBuilder = {
    val safe = sanitizeFreeText(name)
    if (safe != null) b.setAttribute(OperatorName, safe) else b
  }

  // ---- Typed setters for Span (used after a span is active) ------------

  def setWorkflowId(s: Span, id: Long): Span = s.setAttribute(WorkflowId, java.lang.Long.valueOf(id))
  def setExecutionId(s: Span, id: Long): Span = s.setAttribute(ExecutionId, java.lang.Long.valueOf(id))
  def setOperatorId(s: Span, id: String): Span = {
    if (id != null && OperatorIdPattern.matcher(id).matches()) s.setAttribute(OperatorId, id)
    else s
  }
  def setOutcome(s: Span, outcome: String): Span = {
    val safe = sanitizeFreeText(outcome)
    if (safe != null) s.setAttribute(Outcome, safe) else s
  }

  // ---- Pure helpers (exposed for testing) ------------------------------

  /**
   * Strip CR/LF and other C0 control characters from a free-text
   * value, then cap at [[FreeTextMaxLen]]. Returns null for
   * null/empty input (caller skips the setAttribute call).
   */
  def sanitizeFreeText(value: String): String = {
    if (value == null || value.isEmpty) return null
    val stripped = value.filter(c => c >= 0x20 && c != 0x7F)
    if (stripped.isEmpty) null
    else if (stripped.length <= FreeTextMaxLen) stripped
    else stripped.substring(0, FreeTextMaxLen)
  }
}
