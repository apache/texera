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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * Simple token-bucket rate limiter, keyed by an arbitrary string
 * (typically userId or remote IP).
 *
 * Trade-offs vs. a fancier approach:
 *   - In-memory only. A single instance per JVM. A rolling-restart
 *     deploy would clear the buckets — acceptable for the
 *     observability gateway's traffic shape (low QPS, low
 *     consequences for a single missed limit at restart).
 *   - No background thread. Refill happens lazily on the next
 *     [[tryAcquire]] call against the same key. Simpler reasoning,
 *     no scheduler involved.
 *   - Keys never expire from the map. The map is bounded informally
 *     by the number of distinct (user, IP) tuples a deploy sees;
 *     a future PR can add a periodic clean-up if cardinality grows.
 */
class RateLimiter(capacity: Long, refillPerSecond: Double) {

  require(capacity > 0, "capacity must be positive")
  require(refillPerSecond > 0, "refillPerSecond must be positive")

  private val buckets: ConcurrentHashMap[String, AtomicReference[Bucket]] =
    new ConcurrentHashMap[String, AtomicReference[Bucket]]()

  /** Try to consume one token. Returns true on success, false on
   *  rate-limit. Time-aware: the bucket refills based on wall clock
   *  elapsed since the last call. */
  def tryAcquire(key: String, nowMillis: Long = System.currentTimeMillis()): Boolean = {
    val ref = buckets.computeIfAbsent(
      key,
      _ => new AtomicReference[Bucket](Bucket(capacity.toDouble, nowMillis))
    )
    // CAS loop: take the current bucket, refill against now,
    // attempt to debit one token, swap back.
    var done = false
    var allowed = false
    while (!done) {
      val cur = ref.get()
      val elapsedSec = (nowMillis - cur.lastRefillMs).toDouble / 1000.0
      val refilled = math.min(capacity.toDouble, cur.tokens + elapsedSec * refillPerSecond)
      if (refilled >= 1.0) {
        val next = Bucket(refilled - 1.0, nowMillis)
        if (ref.compareAndSet(cur, next)) {
          allowed = true
          done = true
        }
      } else {
        val next = Bucket(refilled, nowMillis)
        if (ref.compareAndSet(cur, next)) {
          allowed = false
          done = true
        }
      }
    }
    allowed
  }

  /** Test-only: drop all in-memory state. */
  private[gateway] def resetForTest(): Unit = buckets.clear()

  private case class Bucket(tokens: Double, lastRefillMs: Long)
}

object RateLimiter {
  /** Default per-user limit per the PR plan: 20 req/s with a 20-
   *  token burst capacity. */
  def defaultPerUser(): RateLimiter = new RateLimiter(capacity = 20, refillPerSecond = 20.0)

  /** Default per-IP limit: looser per-user limit, tighter per-IP
   *  to defend against an attacker burning through a fleet of
   *  accounts. */
  def defaultPerIp(): RateLimiter = new RateLimiter(capacity = 100, refillPerSecond = 50.0)
}
