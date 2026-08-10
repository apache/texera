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
import org.apache.texera.dao.jooq.generated.Tables.{USER, USER_LAST_ACTIVE_TIME}
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{OffsetDateTime, ZoneOffset}

/** Exercises the production singleton's *default* upsert against a real
  * database. [[UserActivityTrackerSpec]] covers the cooldown / CAS / eviction
  * logic with an injected `upsertFn`, so this suite only has to reach the one
  * path that spec cannot: the jOOQ write itself.
  *
  * Kept in its own file so the pure-logic spec stays free of embedded Postgres.
  */
class UserActivityTrackerDbSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  // The singleton's per-uid cooldown map lives as long as the JVM, so uids are
  // drawn fresh per run rather than fixed: a re-run that happened to share a
  // classloader with an earlier one would otherwise find its uids still in
  // cooldown and see no write at all. Each test also claims a uid of its own.
  private val uidBase: Int = 90000 + scala.util.Random.nextInt(900000)
  private val writeUid: Integer = uidBase
  private val cooldownUid: Integer = uidBase + 1
  private val barrierUid: Integer = uidBase + 2

  private val sentinel = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    Seq(writeUid, cooldownUid, barrierUid).foreach { uid =>
      val user = new User
      user.setUid(uid)
      user.setName(s"activity_user_$uid")
      user.setEmail(s"activity_$uid@example.com")
      user.setPassword("password")
      userDao.insert(user)
    }
  }

  override protected def afterAll(): Unit = {
    getDSLContext
      .deleteFrom(USER_LAST_ACTIVE_TIME)
      .where(USER_LAST_ACTIVE_TIME.UID.in(writeUid, cooldownUid, barrierUid))
      .execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.in(writeUid, cooldownUid, barrierUid)).execute()
    shutdownDB()
  }

  private def lastActiveTimeOf(uid: Integer): Option[OffsetDateTime] =
    Option(
      getDSLContext
        .select(USER_LAST_ACTIVE_TIME.LAST_ACTIVE_TIME)
        .from(USER_LAST_ACTIVE_TIME)
        .where(USER_LAST_ACTIVE_TIME.UID.eq(uid))
        .fetchOne()
    ).map(_.value1())

  /** The upsert runs on the tracker's writer thread, so wait for the row it
    * writes. The write is guaranteed to happen; the deadline only bounds how
    * long we are willing to wait for a machine to get to it.
    */
  private def awaitRow(uid: Integer): OffsetDateTime = {
    val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15)
    while (System.nanoTime() < deadline) {
      lastActiveTimeOf(uid) match {
        case Some(ts) => return ts
        case None     => Thread.sleep(20)
      }
    }
    fail(s"no USER_LAST_ACTIVE_TIME row was written for uid $uid")
  }

  "UserActivityTracker singleton" should "write the activity row through the default upsert" in {
    lastActiveTimeOf(writeUid) shouldBe None

    UserActivityTracker.markActive(writeUid)

    awaitRow(writeUid) should not be null
  }

  it should "not write again while the uid is still within its cooldown window" in {
    UserActivityTracker.markActive(cooldownUid)
    awaitRow(cooldownUid)

    // Park a value the upsert would overwrite if it ran a second time.
    getDSLContext
      .update(USER_LAST_ACTIVE_TIME)
      .set(USER_LAST_ACTIVE_TIME.LAST_ACTIVE_TIME, sentinel)
      .where(USER_LAST_ACTIVE_TIME.UID.eq(cooldownUid))
      .execute()

    // Same uid again, immediately: the 5-minute cooldown should drop it.
    UserActivityTracker.markActive(cooldownUid)

    // Barrier: the tracker's writer is a single thread with a FIFO queue, so
    // once a later-submitted write lands, any earlier one has already run.
    // This replaces sleeping-and-hoping with an ordering guarantee.
    UserActivityTracker.markActive(barrierUid)
    awaitRow(barrierUid)

    lastActiveTimeOf(cooldownUid).map(_.toInstant) shouldBe Some(sentinel.toInstant)
  }
}
