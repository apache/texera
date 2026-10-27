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

/**
  * Standard Texera span-attribute keys plus a free-text sanitizer.
  *
  * These are the shared label keys so every callsite tags spans with the
  * same names. Set them with the standard OTel API at the callsite, e.g.
  * ``spanBuilder.setAttribute(SpanAttrs.WorkflowId, id)`` or
  * ``span.setAttribute(SpanAttrs.WorkflowId, id)``. Run any free-text value
  * through [[sanitizeFreeText]] first to strip CRLF and cap its length.
  */
object SpanAttrs {

  /** Maximum length for free-text span attribute values. */
  val FreeTextMaxLen: Int = 256

  // ---- Standard Texera correlation labels ------------------------------

  val WorkflowId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.workflow.id")
  val ExecutionId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.execution.id")
  val ProjectId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.project.id")
  val UserId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.user.id")
  val OperatorId: AttributeKey[String] = AttributeKey.stringKey("texera.operator.id")
  val OperatorName: AttributeKey[String] = AttributeKey.stringKey("texera.operator.name")
  val Outcome: AttributeKey[String] = AttributeKey.stringKey("texera.outcome")

  // ---- Pure helpers (exposed for testing) ------------------------------

  /**
    * Strip CR/LF and other C0 control characters from a free-text
    * value, then cap at [[FreeTextMaxLen]]. Returns null for
    * null/empty input (caller skips the setAttribute call).
    */
  def sanitizeFreeText(value: String): String = {
    if (value == null || value.isEmpty) return null
    val stripped = value.filter(c => c >= 0x20 && c != 0x7f)
    if (stripped.isEmpty) null
    else if (stripped.length <= FreeTextMaxLen) stripped
    else stripped.substring(0, FreeTextMaxLen)
  }
}
