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

class TraceparentValidatorSpec extends AnyFlatSpec with Matchers {

  // ----- traceparent: positive paths -----------------------------------

  "validateTraceparent" should "accept a canonical W3C traceparent" in {
    val good = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    TraceparentValidator.validateTraceparent(good) shouldBe Some(good)
  }

  it should "accept sampled=0 flag" in {
    val good = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
    TraceparentValidator.validateTraceparent(good) shouldBe Some(good)
  }

  // ----- traceparent: negative paths -----------------------------------

  it should "reject null and empty input" in {
    TraceparentValidator.validateTraceparent(null) shouldBe None
    TraceparentValidator.validateTraceparent("") shouldBe None
  }

  it should "reject path-traversal-style spoofing attempts" in {
    TraceparentValidator.validateTraceparent("../../etc/passwd") shouldBe None
    TraceparentValidator.validateTraceparent(
      "00-../../../etc/passwd-b7ad6b7169203331-01"
    ) shouldBe None
  }

  it should "reject the wrong version byte" in {
    // Only version 00 is published. 01, ff, etc. are invalid.
    val bad01 = "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    val badff = "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    TraceparentValidator.validateTraceparent(bad01) shouldBe None
    TraceparentValidator.validateTraceparent(badff) shouldBe None
  }

  it should "reject all-zero trace-id (sentinel for no-value)" in {
    val zero = "00-00000000000000000000000000000000-b7ad6b7169203331-01"
    TraceparentValidator.validateTraceparent(zero) shouldBe None
  }

  it should "reject all-zero span-id (sentinel for no-value)" in {
    val zero = "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01"
    TraceparentValidator.validateTraceparent(zero) shouldBe None
  }

  it should "reject UPPERCASE hex (spec requires lowercase)" in {
    val upper = "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01"
    TraceparentValidator.validateTraceparent(upper) shouldBe None
  }

  it should "reject non-hex characters in trace-id" in {
    val bad = "00-XYZ7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    TraceparentValidator.validateTraceparent(bad) shouldBe None
  }

  it should "reject wrong-length segments" in {
    // trace-id one char short
    val short = "00-af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    // span-id one char long
    val long = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b71692033311-01"
    TraceparentValidator.validateTraceparent(short) shouldBe None
    TraceparentValidator.validateTraceparent(long) shouldBe None
  }

  it should "reject embedded CRLF (header injection)" in {
    val crlf =
      "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\r\nX-Forge: yes"
    TraceparentValidator.validateTraceparent(crlf) shouldBe None
  }

  // ----- tracestate ----------------------------------------------------

  "validateTracestate" should "accept a canonical tracestate" in {
    val ts = "vendor1=hello,vendor2=world"
    TraceparentValidator.validateTracestate(ts) shouldBe Some(ts)
  }

  it should "reject null and empty" in {
    TraceparentValidator.validateTracestate(null) shouldBe None
    TraceparentValidator.validateTracestate("") shouldBe None
  }

  it should "reject tracestate longer than the cap" in {
    val oversize = "a=" + ("b" * (TraceparentValidator.MaxTracestateLength + 1))
    TraceparentValidator.validateTracestate(oversize) shouldBe None
  }

  it should "reject non-ASCII / control characters" in {
    TraceparentValidator.validateTracestate("vendor=hello\u00ffworld") shouldBe None
    TraceparentValidator.validateTracestate("vendor=hello\r\nworld") shouldBe None
    TraceparentValidator.validateTracestate("vendor=hello\u0000world") shouldBe None
  }

  it should "accept tracestate at exactly the cap" in {
    val atCap = "k=" + ("v" * (TraceparentValidator.MaxTracestateLength - 2))
    atCap.length shouldBe TraceparentValidator.MaxTracestateLength
    TraceparentValidator.validateTracestate(atCap) shouldBe Some(atCap)
  }
}
