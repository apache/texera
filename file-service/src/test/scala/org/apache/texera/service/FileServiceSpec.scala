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
import org.apache.texera.service.util.StagedFileCleanupJob
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class FileServiceSpec extends AnyFlatSpec {

  private val service = new FileService()

  "awaitDependency" should "run the operation once and not sleep when it succeeds immediately" in {
    var attempts = 0
    val delays = ListBuffer.empty[Long]
    service.awaitDependency("dep", 6, 200L, delays += _) {
      attempts += 1
    }
    assert(attempts == 1)
    assert(delays.isEmpty)
  }

  it should "run the operation once with the default arguments when it succeeds immediately" in {
    // Exercises the default maxAttempts/initialDelay/sleep parameters: a first-try success
    // returns without ever invoking the (real Thread.sleep) default backoff.
    var attempts = 0
    service.awaitDependency("dep") {
      attempts += 1
    }
    assert(attempts == 1)
  }

  it should "retry until success and double the delay after each failed attempt" in {
    var attempts = 0
    val delays = ListBuffer.empty[Long]
    service.awaitDependency("dep", 6, 200L, delays += _) {
      attempts += 1
      if (attempts < 3) throw new RuntimeException("not reachable yet")
    }
    assert(attempts == 3)
    assert(delays.toList == List(200L, 400L))
  }

  it should "double the delay after every failed attempt up to maxAttempts - 1 sleeps" in {
    var attempts = 0
    val delays = ListBuffer.empty[Long]
    val ex = intercept[RuntimeException] {
      service.awaitDependency("dep", 6, 200L, delays += _) {
        attempts += 1
        throw new RuntimeException("down")
      }
    }
    // 6 attempts means 5 backoff waits following the geometric progression from 200ms.
    assert(attempts == 6)
    assert(delays.toList == List(200L, 400L, 800L, 1600L, 3200L))
    assert(ex.getMessage.contains("after 6 attempts"))
  }

  it should "give up after maxAttempts and preserve the last failure as the cause" in {
    var attempts = 0
    val cause = new RuntimeException("still down")
    val ex = intercept[RuntimeException] {
      service.awaitDependency("dep", 3, 200L, _ => ()) {
        attempts += 1
        throw cause
      }
    }
    assert(attempts == 3)
    assert(ex.getMessage.contains("after 3 attempts"))
    assert(ex.getMessage.contains("dep"))
    assert(ex.getCause eq cause)
  }

  it should "give up immediately without sleeping when maxAttempts is 1" in {
    var attempts = 0
    val delays = ListBuffer.empty[Long]
    val cause = new RuntimeException("still down")
    val ex = intercept[RuntimeException] {
      service.awaitDependency("dep", 1, 200L, delays += _) {
        attempts += 1
        throw cause
      }
    }
    assert(attempts == 1)
    assert(delays.isEmpty)
    assert(ex.getMessage.contains("after 1 attempts"))
    assert(ex.getCause eq cause)
  }

  it should "fail fast and restore the interrupt status when the operation is interrupted" in {
    val ex = intercept[RuntimeException] {
      service.awaitDependency("dep", 6, 200L, _ => ()) {
        throw new InterruptedException("interrupted")
      }
    }
    // Thread.interrupted() both reads and clears the flag, so the interrupt was restored.
    assert(Thread.interrupted())
    assert(ex.getMessage.contains("Interrupted while waiting for dep"))
    assert(ex.getCause.isInstanceOf[InterruptedException])
  }

  it should "fail fast and restore the interrupt status when interrupted while sleeping between attempts" in {
    var attempts = 0
    val ex = intercept[RuntimeException] {
      service.awaitDependency("dep", 6, 200L, _ => throw new InterruptedException("interrupted")) {
        attempts += 1
        throw new RuntimeException("not reachable yet")
      }
    }
    // The operation failed once, then the interrupt arrived during the backoff sleep.
    assert(attempts == 1)
    // Thread.interrupted() both reads and clears the flag, so the interrupt was restored.
    assert(Thread.interrupted())
    assert(ex.getMessage.contains("Interrupted while waiting for dep"))
    assert(ex.getCause.isInstanceOf[InterruptedException])
  }

  it should "succeed on the final allowed attempt without giving up one try too early" in {
    // Boundary for `attempt >= maxAttempts`: the operation only succeeds on the very last
    // attempt, so the loop must not give up prematurely. Expect maxAttempts - 1 backoff waits.
    var attempts = 0
    val delays = ListBuffer.empty[Long]
    service.awaitDependency("dep", 3, 200L, delays += _) {
      attempts += 1
      if (attempts < 3) throw new RuntimeException("not reachable yet")
    }
    assert(attempts == 3)
    assert(delays.toList == List(200L, 400L))
  }

  it should "honor a custom initial delay when computing the backoff progression" in {
    // Guards against the initial delay being hardcoded: starting from 50ms the geometric
    // progression must be 50, 100, 200 rather than the default 200-based sequence.
    var attempts = 0
    val delays = ListBuffer.empty[Long]
    val ex = intercept[RuntimeException] {
      service.awaitDependency("dep", 4, 50L, delays += _) {
        attempts += 1
        throw new RuntimeException("down")
      }
    }
    assert(attempts == 4)
    assert(delays.toList == List(50L, 100L, 200L))
    assert(ex.getMessage.contains("after 4 attempts"))
  }

  it should "include the underlying failure message when giving up" in {
    val ex = intercept[RuntimeException] {
      service.awaitDependency("dataset bucket", 2, 200L, _ => ()) {
        throw new RuntimeException("connection refused")
      }
    }
    assert(ex.getMessage.contains("dataset bucket not ready after 2 attempts"))
    assert(ex.getMessage.contains("connection refused"))
  }

  it should "propagate a non-Exception Throwable immediately without retrying or wrapping it" in {
    // The catch clause only matches Exception, so an Error must escape on the first attempt:
    // it is neither retried nor wrapped in the \"not ready after N attempts\" RuntimeException.
    var attempts = 0
    val delays = ListBuffer.empty[Long]
    val err = intercept[StackOverflowError] {
      service.awaitDependency("dep", 6, 200L, delays += _) {
        attempts += 1
        throw new StackOverflowError("boom")
      }
    }
    assert(attempts == 1)
    assert(delays.isEmpty)
    assert(err.getMessage == "boom")
  }

  // ---------------------------------------------------------------------------
  // registerStagedFileCleanup: conditional wiring of the staged-file cleanup job
  // ---------------------------------------------------------------------------
  /** The StagedFileCleanupJob instances registered on an environment's lifecycle. */
  private def registeredCleanupJobs(environment: Environment): Seq[StagedFileCleanupJob] =
    environment
      .lifecycle()
      .getManagedObjects
      .asScala
      .collect {
        case managed: JettyManaged if managed.getManaged.isInstanceOf[StagedFileCleanupJob] =>
          managed.getManaged.asInstanceOf[StagedFileCleanupJob]
      }
      .toSeq

  "registerStagedFileCleanup" should "manage a StagedFileCleanupJob on the lifecycle when enabled" in {
    val environment = new Environment("test-file-service")
    service.registerStagedFileCleanup(
      environment,
      enabled = true,
      retentionHours = 72,
      intervalMinutes = 60
    )
    assert(registeredCleanupJobs(environment).size == 1)
  }

  it should "register nothing when disabled" in {
    val environment = new Environment("test-file-service")
    service.registerStagedFileCleanup(
      environment,
      enabled = false,
      retentionHours = 72,
      intervalMinutes = 60
    )
    assert(registeredCleanupJobs(environment).isEmpty)
  }

  it should "not construct the job (so not throw) when disabled even with invalid config" in {
    // retentionHours/intervalMinutes are invalid (0), but because enabled = false the job is
    // never constructed, so StagedFileCleanupJob's require(...) is never evaluated and nothing
    // throws. This pins that the enabled check guards the construction, not just the registration.
    val environment = new Environment("test-file-service")
    service.registerStagedFileCleanup(
      environment,
      enabled = false,
      retentionHours = 0,
      intervalMinutes = 0
    )
    assert(registeredCleanupJobs(environment).isEmpty)
  }

  it should "fail fast with IllegalArgumentException when enabled with a non-positive retention" in {
    // A non-positive retentionHours trips StagedFileCleanupJob's require(...) at construction, so a
    // misconfigured-but-enabled cleanup surfaces loudly at startup rather than being swallowed.
    val environment = new Environment("test-file-service")
    assertThrows[IllegalArgumentException] {
      service.registerStagedFileCleanup(
        environment,
        enabled = true,
        retentionHours = 0,
        intervalMinutes = 60
      )
    }
  }
}
