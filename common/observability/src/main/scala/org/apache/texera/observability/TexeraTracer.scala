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

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context

/**
  * Accessor for the Texera OTel tracer.
  *
  * The only thing this adds over calling ``GlobalOpenTelemetry.getTracer``
  * directly is a single instrumentation scope name (``org.apache.texera``),
  * so every Texera-produced span shows up under one logical scope in the
  * backend, separable from anything emitted by transitive libraries.
  *
  * Start and end spans with the standard OTel API at the callsite (see the
  * OpenTelemetry Java demo for the recommended pattern):
  *
  * {{{
  *   val span = TexeraTracer.tracer.spanBuilder("MyClass.myMethod").startSpan()
  *   val scope = span.makeCurrent()
  *   try { ... } catch {
  *     case t: Throwable => span.recordException(t); span.setStatus(ERROR); throw t
  *   } finally { scope.close(); span.end() }
  * }}}
  *
  * When the SDK is disabled, ``GlobalOpenTelemetry.getTracer`` returns a
  * no-op tracer, so calling this is safe at any time.
  */
object TexeraTracer {

  private val InstrumentationScope = "org.apache.texera"

  def tracer: Tracer = GlobalOpenTelemetry.getTracer(InstrumentationScope)

  /**
    * Snapshot the current OTel ``Context`` so async callbacks can
    * re-attach it via ``Context.makeCurrent`` later. Useful at the
    * Scala to Python boundary where the calling thread is not the
    * receiving thread.
    */
  def currentContext: Context = Context.current()
}
