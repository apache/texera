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

package org.apache.texera.service.util

import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTerminationReasonEnum
import org.apache.texera.service.resource.ComputingUnitManagingResource.TerminatedComputingUnitInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[IdleComputingUnitCleanupJob]]. The sweep it runs is covered by
  * `ComputingUnitIdleCleanupSpec`; what is pinned here is the job's own contract — it forwards
  * the configured timeout, reports what it terminated, and never lets a failing round cancel the
  * fixed-delay schedule.
  */
class IdleComputingUnitCleanupJobSpec extends AnyFlatSpec with Matchers {

  private def terminatedUnit(cuid: Int, username: Option[String]) =
    TerminatedComputingUnitInfo(
      cuid = cuid,
      name = s"unit-$cuid",
      uid = 7,
      username = username,
      reason = WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
    )

  "IdleComputingUnitCleanupJob" should "reject a non-positive timeout or interval at construction" in {
    assertThrows[IllegalArgumentException](new IdleComputingUnitCleanupJob(0, 60))
    assertThrows[IllegalArgumentException](new IdleComputingUnitCleanupJob(-1, 60))
    assertThrows[IllegalArgumentException](new IdleComputingUnitCleanupJob(1440, 0))
    assertThrows[IllegalArgumentException](new IdleComputingUnitCleanupJob(1440, -1))
  }

  "runCleanupOnce" should "pass the configured idle timeout to the sweep and return what it terminated" in {
    var seenTimeouts = List.empty[Long]
    val job = new IdleComputingUnitCleanupJob(
      idleTimeoutMinutes = 1440,
      intervalMinutes = 60,
      terminateIdleComputingUnits = timeout => {
        seenTimeouts = seenTimeouts :+ timeout
        List(terminatedUnit(1, Some("owner")), terminatedUnit(2, None))
      }
    )

    job.runCleanupOnce().map(_.cuid) shouldBe List(1, 2)
    seenTimeouts shouldBe List(1440L)
  }

  it should "return empty when the sweep terminates nothing" in {
    val job = new IdleComputingUnitCleanupJob(1440, 60, _ => List.empty)
    job.runCleanupOnce() shouldBe empty
  }

  "runScheduledTick" should "swallow a failing round so the fixed-delay schedule survives" in {
    // An exception escaping the scheduled task cancels the schedule outright, silently stopping
    // every future round, so the tick has to absorb it and let the next round retry.
    val job = new IdleComputingUnitCleanupJob(
      1440,
      60,
      _ => throw new RuntimeException("sweep failed")
    )
    noException should be thrownBy job.runScheduledTick()
  }

  it should "run the sweep on a successful round" in {
    var invocations = 0
    val job = new IdleComputingUnitCleanupJob(
      1440,
      60,
      _ => {
        invocations += 1
        List.empty
      }
    )
    job.runScheduledTick()
    invocations shouldBe 1
  }

  "the job lifecycle" should "allow stop() before start() without throwing" in {
    noException should be thrownBy new IdleComputingUnitCleanupJob(1440, 60, _ => List.empty).stop()
  }

  it should "start() then stop() without throwing" in {
    val job = new IdleComputingUnitCleanupJob(1440, 60, _ => List.empty)
    try {
      noException should be thrownBy job.start()
    } finally {
      // Always stop so a started daemon executor never leaks between tests.
      job.stop()
    }
  }
}
