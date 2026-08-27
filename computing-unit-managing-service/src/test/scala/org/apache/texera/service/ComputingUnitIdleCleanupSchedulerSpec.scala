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

package org.apache.texera.service

import io.dropwizard.core.setup.Environment
import io.dropwizard.lifecycle.setup.{LifecycleEnvironment, ScheduledExecutorServiceBuilder}
import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTerminationReasonEnum
import org.apache.texera.service.resource.ComputingUnitManagingResource.TerminatedComputingUnitInfo
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

class ComputingUnitIdleCleanupSchedulerSpec extends AnyFlatSpec with Matchers {

  "ComputingUnitManagingService.registerIdleComputingUnitCleanup" should "wire the Dropwizard scheduled executor" in {
    val environment = mock(classOf[Environment])
    val lifecycle = mock(classOf[LifecycleEnvironment])
    val builder = mock(classOf[ScheduledExecutorServiceBuilder])
    val executor = mock(classOf[ScheduledExecutorService])
    when(environment.lifecycle()).thenReturn(lifecycle)
    when(lifecycle.scheduledExecutorService("idle-computing-unit-terminator")).thenReturn(builder)
    when(builder.threads(1)).thenReturn(builder)
    when(builder.build()).thenReturn(executor)

    new ComputingUnitManagingService().registerIdleComputingUnitCleanup(
      environment,
      kubernetesComputingUnitEnabled = true,
      idleTimeoutMinutes = 60,
      idleCheckIntervalMinutes = 11,
      terminateIdleComputingUnits = () => List.empty
    )

    verify(executor).scheduleWithFixedDelay(
      any(classOf[Runnable]),
      eqTo(11L),
      eqTo(11L),
      eqTo(TimeUnit.MINUTES)
    )
  }

  it should "schedule cleanup only when Kubernetes idle cleanup is enabled" in {
    var scheduled = false

    new ComputingUnitManagingService().registerIdleComputingUnitCleanup(
      mock(classOf[Environment]),
      kubernetesComputingUnitEnabled = false,
      idleTimeoutMinutes = 60,
      idleCheckIntervalMinutes = 15,
      terminateIdleComputingUnits = () => List.empty,
      logTerminatedUnits = _ => (),
      logCleanupFailure = _ => (),
      scheduleWithFixedDelay = Some((_, _, _, _) => scheduled = true)
    )
    scheduled shouldBe false

    new ComputingUnitManagingService().registerIdleComputingUnitCleanup(
      mock(classOf[Environment]),
      kubernetesComputingUnitEnabled = true,
      idleTimeoutMinutes = 60,
      idleCheckIntervalMinutes = 15,
      terminateIdleComputingUnits = () => List.empty,
      logTerminatedUnits = _ => (),
      logCleanupFailure = _ => (),
      scheduleWithFixedDelay = Some((_, _, _, _) => scheduled = true)
    )
    scheduled shouldBe true
  }

  "ComputingUnitManagingService.registerIdleComputingUnitCleanup" should "schedule cleanup with the configured fixed delay" in {
    var scheduledCommand: Runnable = null
    var scheduledInitialDelay: Long = -1
    var scheduledDelay: Long = -1
    var scheduledTimeUnit: TimeUnit = null
    var cleanupInvocations = 0
    var infoMessages = List.empty[String]
    var failures = List.empty[Throwable]

    new ComputingUnitManagingService().registerIdleComputingUnitCleanup(
      mock(classOf[Environment]),
      kubernetesComputingUnitEnabled = true,
      idleTimeoutMinutes = 60,
      idleCheckIntervalMinutes = 15,
      terminateIdleComputingUnits = () => {
        cleanupInvocations += 1
        List.empty
      },
      logTerminatedUnits = message => infoMessages = infoMessages :+ message,
      logCleanupFailure = throwable => failures = failures :+ throwable,
      scheduleWithFixedDelay = Some((command, initialDelay, delay, unit) => {
        scheduledCommand = command
        scheduledInitialDelay = initialDelay
        scheduledDelay = delay
        scheduledTimeUnit = unit
      })
    )

    scheduledCommand should not be null
    scheduledInitialDelay shouldBe 15
    scheduledDelay shouldBe 15
    scheduledTimeUnit shouldBe TimeUnit.MINUTES

    scheduledCommand.run()

    cleanupInvocations shouldBe 1
    infoMessages shouldBe empty
    failures shouldBe empty
  }

  it should "run the scheduled cleanup command and log terminated units" in {
    var scheduledCommand: Runnable = null
    var cleanupInvocations = 0
    var infoMessages = List.empty[String]
    var failures = List.empty[Throwable]

    new ComputingUnitManagingService().registerIdleComputingUnitCleanup(
      mock(classOf[Environment]),
      kubernetesComputingUnitEnabled = true,
      idleTimeoutMinutes = 60,
      idleCheckIntervalMinutes = 5,
      terminateIdleComputingUnits = () => {
        cleanupInvocations += 1
        List(
          TerminatedComputingUnitInfo(
            cuid = 3,
            name = "scheduled-idle",
            uid = 30,
            username = Some("carol"),
            reason = WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
          )
        )
      },
      logTerminatedUnits = message => infoMessages = infoMessages :+ message,
      logCleanupFailure = throwable => failures = failures :+ throwable,
      scheduleWithFixedDelay = Some((command, _, _, _) => scheduledCommand = command)
    )

    scheduledCommand.run()

    cleanupInvocations shouldBe 1
    infoMessages shouldBe List(
      "Terminated 1 idle Kubernetes computing unit(s): " +
        "cuid=3, name=scheduled-idle, uid=30, username=carol, reason=GARBAGE_COLLECTED"
    )
    failures shouldBe empty
  }

  "ComputingUnitManagingService.runIdleComputingUnitCleanup" should "not log when no idle computing units are terminated" in {
    var infoMessages = List.empty[String]
    var failures = List.empty[Throwable]

    ComputingUnitManagingService.runIdleComputingUnitCleanup(
      () => List.empty,
      message => infoMessages = infoMessages :+ message,
      throwable => failures = failures :+ throwable
    )

    infoMessages shouldBe empty
    failures shouldBe empty
  }

  it should "log terminated idle computing unit details" in {
    var infoMessages = List.empty[String]
    var failures = List.empty[Throwable]

    ComputingUnitManagingService.runIdleComputingUnitCleanup(
      () =>
        List(
          TerminatedComputingUnitInfo(
            cuid = 1,
            name = "idle-a",
            uid = 10,
            username = Some("alice"),
            reason = WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
          ),
          TerminatedComputingUnitInfo(
            cuid = 2,
            name = "idle-b",
            uid = 20,
            username = None,
            reason = WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
          )
        ),
      message => infoMessages = infoMessages :+ message,
      throwable => failures = failures :+ throwable
    )

    infoMessages shouldBe List(
      "Terminated 2 idle Kubernetes computing unit(s): " +
        "cuid=1, name=idle-a, uid=10, username=alice, reason=GARBAGE_COLLECTED; " +
        "cuid=2, name=idle-b, uid=20, username=unknown, reason=GARBAGE_COLLECTED"
    )
    failures shouldBe empty
  }

  it should "log cleanup failures without throwing" in {
    val failure = new RuntimeException("cleanup failed")
    var infoMessages = List.empty[String]
    var failures = List.empty[Throwable]

    noException shouldBe thrownBy {
      ComputingUnitManagingService.runIdleComputingUnitCleanup(
        () => throw failure,
        message => infoMessages = infoMessages :+ message,
        throwable => failures = failures :+ throwable
      )
    }

    infoMessages shouldBe empty
    failures shouldBe List(failure)
  }

  it should "not schedule cleanup when Kubernetes is disabled or the timeout is not positive" in {
    var scheduled = false
    val service = new ComputingUnitManagingService()

    service.registerIdleComputingUnitCleanup(
      mock(classOf[Environment]),
      kubernetesComputingUnitEnabled = false,
      idleTimeoutMinutes = 1,
      scheduleWithFixedDelay = Some((_, _, _, _) => scheduled = true)
    )
    scheduled shouldBe false

    service.registerIdleComputingUnitCleanup(
      mock(classOf[Environment]),
      kubernetesComputingUnitEnabled = true,
      idleTimeoutMinutes = 0,
      scheduleWithFixedDelay = Some((_, _, _, _) => scheduled = true)
    )
    scheduled shouldBe false
  }

  it should "not schedule cleanup when the check interval is not positive" in {
    val service = new ComputingUnitManagingService()

    Seq(0L, -5L).foreach { interval =>
      var scheduled = false
      noException shouldBe thrownBy {
        service.registerIdleComputingUnitCleanup(
          mock(classOf[Environment]),
          kubernetesComputingUnitEnabled = true,
          idleTimeoutMinutes = 60,
          idleCheckIntervalMinutes = interval,
          terminateIdleComputingUnits = () => List.empty,
          scheduleWithFixedDelay = Some((_, _, _, _) => scheduled = true)
        )
      }
      scheduled shouldBe false
    }
  }
}
