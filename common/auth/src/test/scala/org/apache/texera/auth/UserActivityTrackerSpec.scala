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

package org.apache.texera.auth

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Duration, Instant}
import java.util.concurrent.{ConcurrentLinkedQueue, Executor}
import java.util.concurrent.atomic.AtomicReference

class UserActivityTrackerSpec extends AnyFlatSpec with Matchers {

  // Synchronous executor: runnable runs on the calling thread, so the
  // test can observe upsert invocations deterministically.
  private val sameThread: Executor = (cmd: Runnable) => cmd.run()

  private class Recorder {
    val calls = new ConcurrentLinkedQueue[(Integer, Instant)]()
    def upsert(uid: Integer, ts: Instant): Unit = { calls.add((uid, ts)); () }
  }

  private def makeTracker(
      writeInterval: Duration,
      recorder: Recorder,
      clock: AtomicReference[Instant]
  ) =
    new UserActivityTracker(writeInterval, recorder.upsert, sameThread, () => clock.get())

  "UserActivityTracker" should "trigger an upsert on the first call for a uid" in {
    val recorder = new Recorder
    val now = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](now)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(42)

    recorder.calls.size shouldBe 1
    val (uid, ts) = recorder.calls.peek()
    uid shouldBe 42
    ts shouldBe now
  }

  it should "skip upserts within the cooldown window" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(42)
    clock.set(t0.plus(Duration.ofMinutes(2)))
    tracker.markActive(42)
    clock.set(t0.plus(Duration.ofMinutes(4).plusSeconds(59)))
    tracker.markActive(42)

    recorder.calls.size shouldBe 1
  }

  it should "fire another upsert once the cooldown elapses" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(42)
    clock.set(t0.plus(Duration.ofMinutes(5)))
    tracker.markActive(42)

    recorder.calls.size shouldBe 2
  }

  it should "track different uids independently" in {
    val recorder = new Recorder
    val clock = new AtomicReference[Instant](Instant.parse("2026-01-01T00:00:00Z"))
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(1)
    tracker.markActive(2)
    tracker.markActive(3)

    recorder.calls.size shouldBe 3
  }

  it should "treat null uid as a no-op" in {
    val recorder = new Recorder
    val clock = new AtomicReference[Instant](Instant.parse("2026-01-01T00:00:00Z"))
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(null)

    recorder.calls.size shouldBe 0
  }

  it should "evict cooldown entries older than 2 * writeInterval" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(1)
    tracker.markActive(2)
    tracker.cooldownSize shouldBe 2

    // 9 minutes — under 2 * writeInterval (10), nothing evicted
    clock.set(t0.plus(Duration.ofMinutes(9)))
    tracker.evictStale()
    tracker.cooldownSize shouldBe 2

    // 11 minutes — past 2 * writeInterval, both entries evicted
    clock.set(t0.plus(Duration.ofMinutes(11)))
    tracker.evictStale()
    tracker.cooldownSize shouldBe 0
  }

  it should "swallow upsertFn exceptions instead of propagating to the caller" in {
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val throwing: (Integer, Instant) => Unit =
      (_, _) => throw new RuntimeException("simulated DB outage")
    val tracker =
      new UserActivityTracker(Duration.ofMinutes(5), throwing, sameThread, () => clock.get())

    // Must not throw — the wrapper catches NonFatal from upsertFn.
    noException should be thrownBy tracker.markActive(42)
  }
}
