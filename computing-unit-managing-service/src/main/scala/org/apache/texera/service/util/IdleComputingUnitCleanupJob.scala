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

import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.lifecycle.Managed
import org.apache.texera.service.resource.ComputingUnitManagingResource
import org.apache.texera.service.resource.ComputingUnitManagingResource.TerminatedComputingUnitInfo

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
  * Periodically terminates Kubernetes computing units whose last workflow execution activity is
  * older than the idle timeout, reclaiming pods their owners have stopped using.
  *
  * @param idleTimeoutMinutes Idle time (in minutes) after which a unit is terminated.
  * @param intervalMinutes    Delay (in minutes) between cleanup rounds.
  */
class IdleComputingUnitCleanupJob(
    idleTimeoutMinutes: Long,
    intervalMinutes: Long,
    terminateIdleComputingUnits: Long => List[TerminatedComputingUnitInfo] =
      ComputingUnitManagingResource.terminateIdleKubernetesComputingUnits
) extends Managed
    with LazyLogging {

  require(idleTimeoutMinutes > 0, s"idleTimeoutMinutes must be > 0 (got $idleTimeoutMinutes)")
  require(intervalMinutes > 0, s"intervalMinutes must be > 0 (got $intervalMinutes)")

  private var executor: ScheduledExecutorService = _

  override def start(): Unit = {
    executor = Executors.newSingleThreadScheduledExecutor((runnable: Runnable) => {
      val thread = new Thread(runnable, "idle-computing-unit-cleanup")
      thread.setDaemon(true)
      thread
    })
    executor.scheduleWithFixedDelay(
      () => runScheduledTick(),
      // Small fixed initial delay so a restart doesn't postpone the backlog of already-idle units
      // by up to a full interval.
      1L,
      intervalMinutes,
      TimeUnit.MINUTES
    )
  }

  /**
    * Runs one cleanup round for the scheduler. Visible for testing. Catches every Throwable
    * because an exception escaping the scheduled task would cancel the fixed-delay schedule and
    * silently stop all future cleanup rounds.
    */
  private[util] def runScheduledTick(): Unit =
    try {
      runCleanupOnce()
    } catch {
      case t: Throwable => logger.error("Idle computing unit cleanup round failed", t)
    }

  /**
    * Runs a single cleanup round. Logs each terminated unit with its owner so a user who lost a
    * computing unit can be traced in the logs. Idempotent: units already terminated are not
    * revisited, and failures are retried on the next round.
    *
    * @return The units terminated in this round.
    */
  private[util] def runCleanupOnce(): List[TerminatedComputingUnitInfo] = {
    val terminated = terminateIdleComputingUnits(idleTimeoutMinutes)
    if (terminated.nonEmpty) {
      val terminatedDetails = terminated
        .map(unit =>
          // Same owner_* keys the manual termination path logs, so both are grepped alike; the
          // sweep has no acting user, which is what reason=GARBAGE_COLLECTED already says.
          s"cuid=${unit.cuid}, name=${unit.name}, owner_uid=${unit.uid}, " +
            s"owner=${unit.username.getOrElse("unknown")}, reason=${unit.reason.getLiteral}"
        )
        .mkString("; ")
      logger.info(
        s"Terminated ${terminated.size} idle Kubernetes computing unit(s): $terminatedDetails"
      )
    }
    terminated
  }

  override def stop(): Unit = {
    if (executor != null) {
      executor.shutdown()
    }
  }
}
