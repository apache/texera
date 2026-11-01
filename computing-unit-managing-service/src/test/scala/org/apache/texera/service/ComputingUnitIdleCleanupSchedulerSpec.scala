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
import io.dropwizard.lifecycle.JettyManaged
import org.apache.texera.service.util.IdleComputingUnitCleanupJob
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
  * Spec for the conditional wiring of the idle computing unit cleanup job. The sweep itself is
  * covered by `ComputingUnitIdleCleanupSpec`, and the job's scheduling by
  * `IdleComputingUnitCleanupJobSpec`; what is pinned here is only which configurations put a job
  * on the lifecycle.
  */
class ComputingUnitIdleCleanupSchedulerSpec extends AnyFlatSpec with Matchers {

  private val service = new ComputingUnitManagingService()

  /** The IdleComputingUnitCleanupJob instances registered on an environment's lifecycle. */
  private def registeredCleanupJobs(
      environment: Environment
  ): Seq[IdleComputingUnitCleanupJob] =
    environment
      .lifecycle()
      .getManagedObjects
      .asScala
      .collect {
        case managed: JettyManaged
            if managed.getManaged.isInstanceOf[IdleComputingUnitCleanupJob] =>
          managed.getManaged.asInstanceOf[IdleComputingUnitCleanupJob]
      }
      .toSeq

  "registerIdleComputingUnitCleanup" should "manage an IdleComputingUnitCleanupJob on the lifecycle when enabled" in {
    val environment = new Environment("test-computing-unit-managing-service")
    service.registerIdleComputingUnitCleanup(
      environment,
      enabled = true,
      idleTimeoutMinutes = 1440,
      intervalMinutes = 60
    )
    registeredCleanupJobs(environment) should have size 1
  }

  it should "register nothing when disabled" in {
    val environment = new Environment("test-computing-unit-managing-service")
    service.registerIdleComputingUnitCleanup(
      environment,
      enabled = false,
      idleTimeoutMinutes = 1440,
      intervalMinutes = 60
    )
    registeredCleanupJobs(environment) shouldBe empty
  }

  it should "not construct the job (so not throw) when disabled even with invalid config" in {
    // idleTimeoutMinutes/intervalMinutes are invalid (0), but because enabled = false the job is
    // never constructed, so IdleComputingUnitCleanupJob's require(...) is never evaluated and
    // nothing throws. This pins that the enabled check guards construction, not just registration.
    val environment = new Environment("test-computing-unit-managing-service")
    service.registerIdleComputingUnitCleanup(
      environment,
      enabled = false,
      idleTimeoutMinutes = 0,
      intervalMinutes = 0
    )
    registeredCleanupJobs(environment) shouldBe empty
  }

  it should "skip the sweep rather than abort startup when enabled with a non-positive timeout or interval" in {
    // A misconfigured sweep leaves the rest of the service perfectly usable, and the scheduler
    // would reject a non-positive delay outright, so the wiring logs and skips instead of letting
    // the job's require(...) take the whole service down at startup.
    Seq((0L, 60L), (-1L, 60L), (1440L, 0L), (1440L, -1L)).foreach {
      case (idleTimeoutMinutes, intervalMinutes) =>
        val environment = new Environment("test-computing-unit-managing-service")
        noException should be thrownBy service.registerIdleComputingUnitCleanup(
          environment,
          enabled = true,
          idleTimeoutMinutes = idleTimeoutMinutes,
          intervalMinutes = intervalMinutes
        )
        registeredCleanupJobs(environment) shouldBe empty
    }
  }
}
