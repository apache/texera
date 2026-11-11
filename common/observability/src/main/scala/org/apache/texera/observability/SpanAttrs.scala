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
  * Standard Texera span-attribute keys.
  *
  * These are the shared label keys so every callsite tags spans with the
  * same names. Set them with the standard OTel API at the callsite, e.g.
  * `spanBuilder.setAttribute(SpanAttrs.WorkflowId, id)` or
  * `span.setAttribute(SpanAttrs.WorkflowId, id)`.
  */
object SpanAttrs {

  // ---- Standard Texera correlation labels ------------------------------

  val WorkflowId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.workflow.id")
  val ExecutionId: AttributeKey[java.lang.Long] = AttributeKey.longKey("texera.execution.id")
}
