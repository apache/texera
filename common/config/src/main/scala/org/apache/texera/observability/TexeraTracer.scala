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
import io.opentelemetry.api.trace.{Span, SpanBuilder, StatusCode, Tracer}
import io.opentelemetry.context.{Context, Scope}

/**
 * Thin convenience wrapper around the global OTel tracer.
 *
 * Two reasons to go through this rather than calling
 * ``GlobalOpenTelemetry.getTracer`` directly at every callsite:
 *
 *  1. Single instrumentation scope name (``org.apache.texera``) — so
 *     every Texera-produced span shows up under one logical scope in
 *     the backend, separable from anything emitted by transitive
 *     libraries.
 *  2. One ergonomic ``withSpan`` API that handles exception → status,
 *     scope cleanup, and span end in a single try/finally. Callers
 *     don't have to remember the ceremony at every site.
 *
 * When the SDK is disabled, ``GlobalOpenTelemetry.getTracer`` returns
 * a no-op tracer, so calling these methods is safe at any time.
 */
object TexeraTracer {

  private val InstrumentationScope = "org.apache.texera"

  def tracer: Tracer = GlobalOpenTelemetry.getTracer(InstrumentationScope)

  def spanBuilder(name: String): SpanBuilder = tracer.spanBuilder(name)

  /**
   * Run ``block`` inside a fresh span; record exceptions, propagate
   * the right span status, and ensure the span is ended exactly once.
   *
   * Use this for synchronous critical sections. For async (Future-
   * returning) code paths use ``withAsyncSpan`` so the span doesn't
   * close before the async work completes.
   */
  def withSpan[T](name: String, configure: SpanBuilder => SpanBuilder = identity)(
      block: Span => T
  ): T = {
    val span = configure(spanBuilder(name)).startSpan()
    val scope: Scope = span.makeCurrent()
    try {
      block(span)
    } catch {
      case t: Throwable =>
        span.recordException(t)
        span.setStatus(StatusCode.ERROR)
        throw t
    } finally {
      scope.close()
      span.end()
    }
  }

  /**
   * Snapshot the current OTel ``Context`` so async callbacks can
   * re-attach it via ``Context.makeCurrent`` later. Useful at the
   * Scala↔Python boundary where the calling thread is not the
   * receiving thread.
   */
  def currentContext: Context = Context.current()
}
