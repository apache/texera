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
import java.util.concurrent.{ConcurrentHashMap, Executor, Executors}

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
) {
  private val lastClaimed = new ConcurrentHashMap[Integer, Instant]()

  /** Record the user as active. Lock-free; performs at most one upsert per
    * uid per `writeInterval`. Safe to call from any thread.
    */
  def markActive(uid: Integer): Unit = {
    if (uid == null) return
    val now = clock()
    val prev = lastClaimed.get(uid)
    if (prev != null && Duration.between(prev, now).compareTo(writeInterval) < 0) return

    // CAS to claim the write slot for this uid. If another thread won the
    // race, drop this call.
    val claimed =
      if (prev == null) lastClaimed.putIfAbsent(uid, now) == null
      else lastClaimed.replace(uid, prev, now)
    if (!claimed) return

    executor.execute(() => upsertFn(uid, now))
  }
}

object UserActivityTracker extends LazyLogging {

  private val WRITE_INTERVAL: Duration = Duration.ofMinutes(5)

  private val writer: Executor = Executors.newSingleThreadExecutor((r: Runnable) => {
    val t = new Thread(r, "user-activity-writer")
    t.setDaemon(true)
    t
  })

  private val instance = new UserActivityTracker(
    WRITE_INTERVAL,
    defaultUpsert,
    writer,
    () => Instant.now()
  )

  /** Production entry point. Delegates to the singleton tracker. */
  def markActive(uid: Integer): Unit = instance.markActive(uid)

  private def defaultUpsert(uid: Integer, ts: Instant): Unit = {
    try {
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
    } catch {
      case e: Throwable =>
        // Tracking is best-effort; never propagate failures.
        logger.warn(s"USER_LAST_ACTIVE_TIME upsert for uid=$uid failed: ${e.getMessage}")
    }
  }
}
