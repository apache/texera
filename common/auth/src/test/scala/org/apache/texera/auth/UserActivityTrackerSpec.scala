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
import java.util.concurrent.{
  ConcurrentLinkedQueue,
  CountDownLatch,
  Executor,
  RejectedExecutionException
}
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

  it should "swallow exceptions thrown before the write is dispatched" in {
    val recorder = new Recorder
    // a clock that throws forces the failure in markActive before executor.execute
    val tracker =
      new UserActivityTracker(
        Duration.ofMinutes(5),
        recorder.upsert,
        sameThread,
        () => throw new RuntimeException("clock boom")
      )

    noException should be thrownBy tracker.markActive(7)
    recorder.calls.size shouldBe 0 // the write was never dispatched
  }

  it should "swallow exceptions thrown by evictStale" in {
    val recorder = new Recorder
    val tracker =
      new UserActivityTracker(
        Duration.ofMinutes(5),
        recorder.upsert,
        sameThread,
        () => throw new RuntimeException("clock boom")
      )

    noException should be thrownBy tracker.evictStale()
  }

  it should "not create a cooldown entry for a null uid" in {
    val recorder = new Recorder
    val clock = new AtomicReference[Instant](Instant.parse("2026-01-01T00:00:00Z"))
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(null)

    tracker.cooldownSize shouldBe 0
  }

  it should "keep exactly one cooldown entry per uid across repeated calls" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(42)
    clock.set(t0.plus(Duration.ofMinutes(1)))
    tracker.markActive(42)
    clock.set(t0.plus(Duration.ofMinutes(6)))
    tracker.markActive(42)

    recorder.calls.size shouldBe 2
    tracker.cooldownSize shouldBe 1
  }

  it should "stamp each upsert with the clock value at claim time" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val t1 = t0.plus(Duration.ofMinutes(7))
    val clock = new AtomicReference[Instant](t0)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(42)
    // a suppressed call in between must not change the recorded timestamps
    clock.set(t0.plus(Duration.ofMinutes(3)))
    tracker.markActive(42)
    clock.set(t1)
    tracker.markActive(42)

    recorder.calls.poll() shouldBe ((42, t0))
    recorder.calls.poll() shouldBe ((42, t1))
    recorder.calls.size shouldBe 0
  }

  it should "evict only the stale entries and retain the fresh ones" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(1)
    clock.set(t0.plus(Duration.ofMinutes(8)))
    tracker.markActive(2)
    tracker.cooldownSize shouldBe 2

    // cutoff = t0 + 11min - 10min = t0 + 1min: uid 1 (t0) is stale, uid 2 (t0+8min) is not
    clock.set(t0.plus(Duration.ofMinutes(11)))
    tracker.evictStale()
    tracker.cooldownSize shouldBe 1

    // uid 2 is still in cooldown, so it must not produce a second write
    tracker.markActive(2)
    recorder.calls.size shouldBe 2
  }

  it should "retain an entry that is exactly 2 * writeInterval old" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(1)

    // cutoff == t0 exactly; the boundary entry is not "before" the cutoff
    clock.set(t0.plus(Duration.ofMinutes(10)))
    tracker.evictStale()
    tracker.cooldownSize shouldBe 1

    // one millisecond past the boundary it is evicted
    clock.set(t0.plus(Duration.ofMinutes(10)).plusMillis(1))
    tracker.evictStale()
    tracker.cooldownSize shouldBe 0
  }

  it should "tolerate evictStale on an empty tracker" in {
    val recorder = new Recorder
    val clock = new AtomicReference[Instant](Instant.parse("2026-01-01T00:00:00Z"))
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    noException should be thrownBy tracker.evictStale()

    tracker.cooldownSize shouldBe 0
    recorder.calls.size shouldBe 0
  }

  it should "re-claim a uid whose entry was evicted" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(1)
    clock.set(t0.plus(Duration.ofMinutes(11)))
    tracker.evictStale()
    tracker.cooldownSize shouldBe 0

    tracker.markActive(1)

    recorder.calls.size shouldBe 2
    tracker.cooldownSize shouldBe 1
  }

  it should "perform exactly one upsert when many threads race on the same uid" in {
    val recorder = new Recorder
    val clock = new AtomicReference[Instant](Instant.parse("2026-01-01T00:00:00Z"))
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    val start = new CountDownLatch(1)
    val threads = (1 to 16).map(_ =>
      new Thread(() => {
        start.await()
        tracker.markActive(99)
      })
    )
    threads.foreach(_.start())
    start.countDown()
    // bounded join: a stuck thread fails the test instead of hanging the suite
    threads.foreach(_.join(5000))
    threads.zipWithIndex.foreach {
      case (t, i) =>
        withClue(s"thread $i did not finish within 5s: ")(t.isAlive shouldBe false)
    }

    // the CAS claim lets a single caller through; the rest are dropped
    recorder.calls.size shouldBe 1
    tracker.cooldownSize shouldBe 1
  }

  it should "upsert on every call when the write interval is zero" in {
    val recorder = new Recorder
    val clock = new AtomicReference[Instant](Instant.parse("2026-01-01T00:00:00Z"))
    val tracker = makeTracker(Duration.ZERO, recorder, clock)

    tracker.markActive(42)
    tracker.markActive(42)
    tracker.markActive(42)

    recorder.calls.size shouldBe 3
  }

  it should "suppress the upsert when the clock moves backwards" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val tracker = makeTracker(Duration.ofMinutes(5), recorder, clock)

    tracker.markActive(42)
    clock.set(t0.minus(Duration.ofMinutes(30)))
    tracker.markActive(42)

    // a negative elapsed time compares below the interval, so nothing is written
    recorder.calls.size shouldBe 1
    tracker.cooldownSize shouldBe 1
  }

  it should "keep the cooldown claim when the executor drops the write" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val dropping: Executor = (_: Runnable) => ()
    val tracker =
      new UserActivityTracker(Duration.ofMinutes(5), recorder.upsert, dropping, () => clock.get())

    tracker.markActive(42)
    tracker.cooldownSize shouldBe 1

    // the claim stands even though the write never ran, so the cooldown still holds
    clock.set(t0.plus(Duration.ofMinutes(2)))
    tracker.markActive(42)

    recorder.calls.size shouldBe 0
    tracker.cooldownSize shouldBe 1
  }

  it should "swallow a rejection thrown by the executor" in {
    val recorder = new Recorder
    val clock = new AtomicReference[Instant](Instant.parse("2026-01-01T00:00:00Z"))
    val rejecting: Executor = (_: Runnable) => throw new RejectedExecutionException("queue full")
    val tracker =
      new UserActivityTracker(Duration.ofMinutes(5), recorder.upsert, rejecting, () => clock.get())

    noException should be thrownBy tracker.markActive(42)

    recorder.calls.size shouldBe 0
    // the slot was claimed before the dispatch attempt failed
    tracker.cooldownSize shouldBe 1
  }

  "UserActivityTracker singleton" should "treat a null uid as a no-op without touching the DB" in {
    noException should be thrownBy UserActivityTracker.markActive(null)
  }
}
