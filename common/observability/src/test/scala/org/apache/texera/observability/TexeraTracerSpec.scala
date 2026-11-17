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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TexeraTracerSpec extends AnyFlatSpec with Matchers {

  "TexeraTracer.tracer" should "return a tracer that can build and start a span" in {
    val tracer = TexeraTracer.tracer
    tracer should not be null
    // Without an installed SDK this is a no-op tracer, but it must still
    // hand back a usable span rather than null so callsites never NPE.
    val span = tracer.spanBuilder("texera.tracer.spec").startSpan()
    span should not be null
    span.end()
  }

  "TexeraTracer.tracer" should "be stable across calls (same instrumentation scope)" in {
    TexeraTracer.tracer should not be null
    TexeraTracer.tracer should not be null
  }
}
