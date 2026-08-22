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

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.USER_LAST_ACTIVE_TIME
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import java.util.concurrent.{
  ConcurrentLinkedQueue,
  CountDownLatch,
  Executor,
  RejectedExecutionException
}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

class UserActivityTrackerSpec
    extends AnyFlatSpec
    with Matchers
    with Eventually
    with BeforeAndAfterAll
    with MockTexeraDB {

  // Synchronous executor: runnable runs on the calling thread, so the
  // test can observe upsert invocations deterministically.
  private val sameThread: Executor = (cmd: Runnable) => cmd.run()

  /** Same calling-thread semantics as [[sameThread]], but it keeps whatever escapes
    * the submitted task. That is what tells "swallowed inside the task, by the
    * wrapper around upsertFn" apart from "swallowed one level up by markActive's own
    * catch" -- both look identical to the caller.
    */
  private class CapturingExecutor extends Executor {
    val escaped = new AtomicReference[Throwable]()

    override def execute(cmd: Runnable): Unit =
      try cmd.run()
      catch { case t: Throwable => escaped.set(t) }
  }

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

  // -- singleton fixture ------------------------------------------------------
  //
  // The `UserActivityTracker` object writes through `defaultUpsert`, which needs a
  // real DSLContext, so the singleton cases run against MockTexeraDB's embedded
  // Postgres. Its cooldown is per-uid and per-JVM, so each singleton case owns a
  // distinct uid: a second call for the same uid inside the production 5-minute
  // window would be suppressed no matter what the case did.
  //
  // uid 8810 has no activity row -> plain INSERT.
  // uid 8820 is pre-seeded -> ON CONFLICT ... DO UPDATE.
  //
  // The two are ten apart on purpose: an off-by-one on the uid written by production
  // code then lands on a uid that exists in neither fixture, so it fails loudly on the
  // foreign key instead of quietly redirecting one case's write onto the other case's
  // row -- which would abort the sibling case on its own clobbered precondition and
  // credit the kill to the wrong test.
  //
  // NOT verified anywhere in this spec: WRITE_INTERVAL's value, WRITER_QUEUE_CAPACITY,
  // the DiscardOldest policy, and the periodic cleanup scheduler. Every line of the
  // companion object runs during class initialisation, so a 100% line figure on this
  // file must not be read as 100% verified.
  private val insertUid: Integer = 8810
  private val updateUid: Integer = 8820
  // used by the queued-write case: 8830's insert is deliberately blocked so that
  // 8840's write cannot start until the test lets go of a table lock.
  private val blockerUid: Integer = 8830
  private val delayedUid: Integer = 8840
  private val seededInstant: Instant = Instant.parse("2001-02-03T04:05:06Z")

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(50, Millis))

  override def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    // user_last_active_time.uid is a FK onto "user"(uid), so every fixture uid needs a
    // parent row.
    Seq(
      insertUid -> "activity-insert",
      updateUid -> "activity-update",
      blockerUid -> "activity-blocker",
      delayedUid -> "activity-delayed"
    ).foreach {
      case (uid, name) =>
        val user = new User
        user.setUid(uid)
        user.setName(name)
        userDao.insert(user)
    }
    getDSLContext
      .insertInto(USER_LAST_ACTIVE_TIME)
      .set(USER_LAST_ACTIVE_TIME.UID, updateUid)
      .set(
        USER_LAST_ACTIVE_TIME.LAST_ACTIVE_TIME,
        OffsetDateTime.ofInstant(seededInstant, ZoneOffset.UTC)
      )
      .execute()
  }

  override def afterAll(): Unit = closeConnectionPool()

  private def readLastActive(uid: Integer): Option[Instant] =
    Option(
      getDSLContext
        .select(USER_LAST_ACTIVE_TIME.LAST_ACTIVE_TIME)
        .from(USER_LAST_ACTIVE_TIME)
        .where(USER_LAST_ACTIVE_TIME.UID.eq(uid))
        .fetchOne()
    ).flatMap(record => Option(record.value1())).map(_.toInstant)

  private def activityRowCount(uid: Integer): Int =
    getDSLContext.fetchCount(USER_LAST_ACTIVE_TIME, USER_LAST_ACTIVE_TIME.UID.eq(uid))

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

  // Both non-fatal shapes are exercised at every catch site: a RuntimeException and a
  // *checked* IOException. The checked one is the shape that separates the actual
  // predicate, `case NonFatal(e)`, from the narrower `case e: RuntimeException` -- Scala
  // throws checked exceptions without declaring them, so the lambda shapes are unchanged.
  private val nonFatalShapes: Seq[Throwable] = Seq(
    new RuntimeException("simulated outage"),
    new java.io.IOException("simulated checked outage")
  )

  it should "swallow upsertFn exceptions instead of propagating to the caller" in {
    nonFatalShapes.foreach { boom =>
      withClue(s"upsertFn threw $boom: ") {
        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        val clock = new AtomicReference[Instant](t0)
        val executor = new CapturingExecutor
        val throwing: (Integer, Instant) => Unit = (_, _) => throw boom
        val tracker =
          new UserActivityTracker(Duration.ofMinutes(5), throwing, executor, () => clock.get())

        // Must not throw — the wrapper catches NonFatal from upsertFn.
        noException should be thrownBy tracker.markActive(42)
        // And it is that wrapper, inside the task, that catches it: nothing escapes the
        // submitted runnable to be mopped up by markActive's own catch instead.
        Option(executor.escaped.get()) shouldBe None
      }
    }
  }

  it should "swallow exceptions thrown before the write is dispatched" in {
    nonFatalShapes.foreach { boom =>
      withClue(s"clock threw $boom: ") {
        val recorder = new Recorder
        // a clock that throws forces the failure in markActive before executor.execute
        val tracker =
          new UserActivityTracker(
            Duration.ofMinutes(5),
            recorder.upsert,
            sameThread,
            () => throw boom
          )

        noException should be thrownBy tracker.markActive(7)
        recorder.calls.size shouldBe 0 // the write was never dispatched
      }
    }
  }

  it should "swallow exceptions thrown by evictStale" in {
    nonFatalShapes.foreach { boom =>
      withClue(s"clock threw $boom: ") {
        val recorder = new Recorder
        val tracker =
          new UserActivityTracker(
            Duration.ofMinutes(5),
            recorder.upsert,
            sameThread,
            () => throw boom
          )

        noException should be thrownBy tracker.evictStale()
      }
    }
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

  it should "let a fatal error from the clock escape markActive" in {
    val recorder = new Recorder
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    // The catch is `NonFatal`, not `Throwable`: a transient clock/DB hiccup is
    // swallowed (cases above), but an InterruptedException or an OOM has to reach
    // the caller instead of being logged and dropped as if it were transient.
    //
    // The first clock read succeeds on purpose. `clock()` is the first statement in
    // markActive's try, so after a throw on the very first call every count is zero no
    // matter what the rest of the method does; letting one call through first makes the
    // trailing assertions observations about state the tracker really built.
    val reads = new AtomicInteger()
    val tracker =
      new UserActivityTracker(
        Duration.ofMinutes(5),
        recorder.upsert,
        sameThread,
        () =>
          if (reads.incrementAndGet() == 1) t0
          else throw new InterruptedException("fatal clock")
      )

    tracker.markActive(5150)
    recorder.calls.size shouldBe 1
    tracker.cooldownSize shouldBe 1

    an[InterruptedException] should be thrownBy tracker.markActive(5150)

    // the escape left the first call's write and its cooldown claim exactly as they were
    recorder.calls.size shouldBe 1
    tracker.cooldownSize shouldBe 1
  }

  it should "let a fatal error from upsertFn escape markActive" in {
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    val clock = new AtomicReference[Instant](t0)
    val fatal: (Integer, Instant) => Unit =
      (_, _) => throw new InterruptedException("fatal upsert")
    val tracker =
      new UserActivityTracker(Duration.ofMinutes(5), fatal, sameThread, () => clock.get())

    // The same NonFatal-not-Throwable contract at the third catch site, the wrapper
    // around upsertFn: on this synchronous executor a fatal from the write passes
    // through both catches and out to the caller.
    an[InterruptedException] should be thrownBy tracker.markActive(4242)

    // the slot was claimed before the write was dispatched
    tracker.cooldownSize shouldBe 1
  }

  it should "let a fatal error from the clock escape evictStale" in {
    val recorder = new Recorder
    val tracker =
      new UserActivityTracker(
        Duration.ofMinutes(5),
        recorder.upsert,
        sameThread,
        () => throw new InterruptedException("fatal clock")
      )

    an[InterruptedException] should be thrownBy tracker.evictStale()
  }

  "UserActivityTracker singleton" should "treat a null uid as a no-op without touching the DB" in {
    noException should be thrownBy UserActivityTracker.markActive(null)
  }

  it should "insert a last-active row for a uid that has none" in {
    // No row at all, which is what makes this the INSERT arm rather than the DO UPDATE
    // arm. `readLastActive` alone would not establish that: last_active_time is
    // nullable, so a row carrying a NULL also reads back as None.
    activityRowCount(insertUid) shouldBe 0

    val beforeCall = Instant.now()
    UserActivityTracker.markActive(insertUid)
    // Taken here, not after the `eventually` below: the accepted window is then the
    // duration of the markActive call itself rather than however long the poll took,
    // which is what binds the stored value to the instant handed to the writer.
    val afterCall = Instant.now()

    // The singleton dispatches the upsert onto its own writer thread, so the row
    // appears asynchronously.
    val stored = eventually {
      readLastActive(insertUid).getOrElse(fail(s"no activity row was written for $insertUid"))
    }

    // The stamped instant is the claim time, taken synchronously inside markActive, so
    // it lands in [beforeCall, afterCall]. The millisecond of slack absorbs Postgres'
    // microsecond rounding, nothing more; a timestamp taken at write time instead
    // lands after this window, because opening a DSLContext and round-tripping the
    // insert is never a sub-millisecond affair.
    withClue(s"stored=$stored window=[$beforeCall, $afterCall]: ") {
      stored.isBefore(beforeCall.minusMillis(1)) shouldBe false
      stored.isAfter(afterCall.plusMillis(1)) shouldBe false
    }
  }

  it should "stamp a queued write with its claim time rather than the time it runs" in {
    // A tight wall-clock window around markActive is not enough to separate claim time
    // from write time: the writer thread normally picks the task up within a
    // millisecond, so both land inside any window wide enough to be stable. The
    // difference only becomes visible when the write is made to WAIT, which is also the
    // case that matters -- under a DB stall, tasks sit in the bounded writer queue, and
    // what has to be recorded is when the user was seen, not when the row finally
    // landed.
    //
    // The stall is manufactured with an EXCLUSIVE table lock held on a raw connection
    // (outside the pool the writer borrows from): it blocks blockerUid's insert, and the
    // single-threaded writer therefore cannot even start delayedUid's task until the
    // lock is released.
    val lockConn = newRawConnection()
    val beforeCall = Instant.now()
    val afterCall =
      try {
        lockConn.setAutoCommit(false)
        val stmt = lockConn.createStatement()
        try stmt.execute("LOCK TABLE texera_db.user_last_active_time IN EXCLUSIVE MODE")
        finally stmt.close()

        UserActivityTracker.markActive(blockerUid)
        // give the writer thread time to start the insert and block on the lock
        Thread.sleep(500)
        UserActivityTracker.markActive(delayedUid)
        val callReturned = Instant.now()
        // keep the writer parked well past the tolerance asserted below
        Thread.sleep(1500)
        callReturned
      } finally {
        lockConn.rollback()
        lockConn.close()
      }

    val stored = eventually {
      readLastActive(delayedUid).getOrElse(fail(s"no activity row was written for $delayedUid"))
    }

    // The write ran at least a second and a half after the call returned, so a stamp
    // taken at write time lands far outside this window while the claim-time stamp the
    // caller handed over is still inside it.
    withClue(s"stored=$stored window=[$beforeCall, $afterCall]: ") {
      stored.isBefore(beforeCall.minusMillis(1)) shouldBe false
      stored.isAfter(afterCall.plusMillis(100)) shouldBe false
    }
  }

  it should "overwrite the existing last-active row instead of failing on its primary key" in {
    // uid is the primary key, so a write for a user that already has a row can only
    // succeed through the ON CONFLICT ... DO UPDATE arm. The specific seeded value is
    // what establishes that premise -- and what the new write has to replace.
    readLastActive(updateUid) shouldBe Some(seededInstant)

    val beforeCall = Instant.now()
    UserActivityTracker.markActive(updateUid)
    val afterCall = Instant.now()

    val stored = eventually {
      val current =
        readLastActive(updateUid).getOrElse(fail(s"activity row for $updateUid disappeared"))
      current should not be seededInstant
      current
    }

    withClue(s"stored=$stored window=[$beforeCall, $afterCall]: ") {
      stored.isBefore(beforeCall.minusMillis(1)) shouldBe false
      stored.isAfter(afterCall.plusMillis(1)) shouldBe false
    }
  }
}
