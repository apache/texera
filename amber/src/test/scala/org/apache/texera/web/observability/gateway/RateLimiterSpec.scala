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

package org.apache.texera.web.observability.gateway

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RateLimiterSpec extends AnyFlatSpec with Matchers {

  "RateLimiter" should "permit up to `capacity` consecutive calls when no time elapses" in {
    val rl = new RateLimiter(capacity = 5, refillPerSecond = 1.0)
    (1 to 5).foreach { _ =>
      rl.tryAcquire("u1", nowMillis = 0L) shouldBe true
    }
    rl.tryAcquire("u1", nowMillis = 0L) shouldBe false
  }

  it should "refill at the specified rate per second" in {
    val rl = new RateLimiter(capacity = 2, refillPerSecond = 1.0)
    rl.tryAcquire("u1", nowMillis = 0L) shouldBe true
    rl.tryAcquire("u1", nowMillis = 0L) shouldBe true
    rl.tryAcquire("u1", nowMillis = 0L) shouldBe false
    // After 1 second, one token should be back.
    rl.tryAcquire("u1", nowMillis = 1000L) shouldBe true
    rl.tryAcquire("u1", nowMillis = 1000L) shouldBe false
  }

  it should "track buckets per-key independently" in {
    val rl = new RateLimiter(capacity = 1, refillPerSecond = 0.1)
    rl.tryAcquire("u1", nowMillis = 0L) shouldBe true
    rl.tryAcquire("u1", nowMillis = 0L) shouldBe false
    // Different key — fresh bucket.
    rl.tryAcquire("u2", nowMillis = 0L) shouldBe true
  }

  it should "cap refilled tokens at capacity (no overflow)" in {
    val rl = new RateLimiter(capacity = 3, refillPerSecond = 100.0)
    // After 10 seconds of inactivity, 1000 tokens "would" refill —
    // but capacity should clamp to 3.
    rl.tryAcquire("u1", nowMillis = 10_000L) shouldBe true
    rl.tryAcquire("u1", nowMillis = 10_000L) shouldBe true
    rl.tryAcquire("u1", nowMillis = 10_000L) shouldBe true
    rl.tryAcquire("u1", nowMillis = 10_000L) shouldBe false
  }

  it should "reject zero/negative capacity at construction time" in {
    an[IllegalArgumentException] should be thrownBy new RateLimiter(0, 1.0)
    an[IllegalArgumentException] should be thrownBy new RateLimiter(-1, 1.0)
    an[IllegalArgumentException] should be thrownBy new RateLimiter(10, 0.0)
  }

  it should "use the default per-user limit of 20 req/s with capacity 20" in {
    val rl = RateLimiter.defaultPerUser()
    (1 to 20).foreach { _ =>
      rl.tryAcquire("u1", nowMillis = 0L) shouldBe true
    }
    rl.tryAcquire("u1", nowMillis = 0L) shouldBe false
  }
}
