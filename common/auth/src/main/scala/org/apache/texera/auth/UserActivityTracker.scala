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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.USER_LAST_ACTIVE_TIME

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import java.util.concurrent.{
  ArrayBlockingQueue,
  ConcurrentHashMap,
  Executor,
  Executors,
  ScheduledExecutorService,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}
import scala.util.control.NonFatal

/** Per-uid activity timestamp recorder. The actual DB upsert is throttled
  * by a per-uid in-memory cooldown so that a user hitting the API at high
  * RPS produces at most one USER_LAST_ACTIVE_TIME write per
  * `writeInterval`. The upsert itself runs on the supplied `executor` so
  * request threads never wait on DB latency.
  *
  * Class form (with injectable upsert / executor / clock) exists so the
  * cooldown/CAS logic can be unit-tested without a DB. The companion
  * object [[UserActivityTracker]] is the production singleton.
  */
class UserActivityTracker(
    writeInterval: Duration,
    upsertFn: (Integer, Instant) => Unit,
    executor: Executor,
    clock: () => Instant
) extends LazyLogging {

  private val lastClaimed = new ConcurrentHashMap[Integer, Instant]()

  // Eviction window: an entry is stale once 2*writeInterval has passed
  // since its last claim. The factor keeps in-cooldown entries safe from
  // eviction while still bounding `lastClaimed` for users who have gone
  // away.
  private val staleAfter: Duration = writeInterval.multipliedBy(2)

  /** Record the user as active. Lock-free; performs at most one upsert per
    * uid per `writeInterval`. Never propagates failures to the caller.
    */
  def markActive(uid: Integer): Unit = {
    if (uid == null) return
    try {
      val now = clock()
      val prev = lastClaimed.get(uid)
      if (prev != null && Duration.between(prev, now).compareTo(writeInterval) < 0) return

      // CAS to claim the write slot for this uid. If another thread won
      // the race, drop this call.
      val claimed =
        if (prev == null) lastClaimed.putIfAbsent(uid, now) == null
        else lastClaimed.replace(uid, prev, now)
      if (!claimed) return

      executor.execute(() =>
        try upsertFn(uid, now)
        catch {
          case NonFatal(e) =>
            logger.warn(s"User activity upsert failed (uid=$uid)", e)
        }
      )
    } catch {
      case NonFatal(e) =>
        logger.warn(s"markActive failed (uid=$uid)", e)
    }
  }

  /** Drop entries whose last-claimed time is older than `2 * writeInterval`.
    * Bounds `lastClaimed` for long-lived processes with many distinct uids.
    * Safe to call concurrently with [[markActive]].
    */
  def evictStale(): Unit = {
    try {
      val cutoff = clock().minus(staleAfter)
      lastClaimed.entrySet().removeIf(e => e.getValue.isBefore(cutoff))
    } catch {
      case NonFatal(e) => logger.warn("evictStale failed", e)
    }
  }

  /** Visible for tests. */
  private[auth] def cooldownSize: Int = lastClaimed.size()
}

object UserActivityTracker extends LazyLogging {

  private val WRITE_INTERVAL: Duration = Duration.ofMinutes(5)
  // Bounded queue: under DB stalls or write storms, oldest pending tasks
  // are dropped (DiscardOldest). The next request from the same uid will
  // re-claim and re-write once cooldown elapses, so dropping a stale
  // pending write does not lose the activity signal long-term.
  private val WRITER_QUEUE_CAPACITY = 256

  private val writer: Executor = new ThreadPoolExecutor(
    1,
    1,
    0L,
    TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](WRITER_QUEUE_CAPACITY),
    daemonThreadFactory("user-activity-writer"),
    new ThreadPoolExecutor.DiscardOldestPolicy
  )

  private val instance = new UserActivityTracker(
    WRITE_INTERVAL,
    defaultUpsert,
    writer,
    () => Instant.now()
  )

  // Periodic eviction of stale uid entries, running once per WRITE_INTERVAL.
  private val cleanup: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(daemonThreadFactory("user-activity-cleanup"))
  cleanup.scheduleAtFixedRate(
    () => instance.evictStale(),
    WRITE_INTERVAL.toMillis,
    WRITE_INTERVAL.toMillis,
    TimeUnit.MILLISECONDS
  )

  /** Production entry point. Delegates to the singleton tracker. */
  def markActive(uid: Integer): Unit = instance.markActive(uid)

  private def defaultUpsert(uid: Integer, ts: Instant): Unit = {
    val ctx = SqlServer.getInstance().createDSLContext()
    val odt = OffsetDateTime.ofInstant(ts, ZoneOffset.UTC)
    ctx
      .insertInto(USER_LAST_ACTIVE_TIME)
      .set(USER_LAST_ACTIVE_TIME.UID, uid)
      .set(USER_LAST_ACTIVE_TIME.LAST_ACTIVE_TIME, odt)
      .onConflict(USER_LAST_ACTIVE_TIME.UID)
      .doUpdate()
      .set(USER_LAST_ACTIVE_TIME.LAST_ACTIVE_TIME, odt)
      .execute()
  }

  private def daemonThreadFactory(name: String): ThreadFactory =
    (r: Runnable) => {
      val t = new Thread(r, name)
      t.setDaemon(true)
      t
    }
}
