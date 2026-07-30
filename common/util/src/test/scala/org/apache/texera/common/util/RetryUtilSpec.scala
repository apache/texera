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

package org.apache.texera.common.util

import org.apache.texera.common.util.RetryUtil.RetryAttempt
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ListBuffer
import scala.util.control.ControlThrowable

/**
  * Contract of the shared blocking backoff retry. `sleep` is injected everywhere so the backoff
  * progression is asserted exactly without any test waiting.
  *
  * These cases are the union of what the two hand-rolled loops this util replaced were tested for
  * (`LakeFSStorageClient.retryWithBackoff`, `FileService.awaitDependency`), plus the interrupt
  * during a backoff sleep, which the LakeFS copy did not handle.
  */
class RetryUtilSpec extends AnyFlatSpec {

  private def noRetryHook: RetryAttempt => Unit = _ => ()

  "RetryUtil.withBackoff" should "return the operation's value on first success without sleeping" in {
    val delays = ListBuffer.empty[Long]
    var attempts = 0
    val result = RetryUtil.withBackoff("reach the store", 5, 200L, noRetryHook, delays += _) {
      attempts += 1
      "value"
    }
    assert(result == "value")
    assert(attempts == 1)
    assert(delays.isEmpty)
  }

  it should "retry until success and double the delay after each failed attempt" in {
    val delays = ListBuffer.empty[Long]
    var attempts = 0
    val result = RetryUtil.withBackoff("reach the store", 5, 200L, noRetryHook, delays += _) {
      attempts += 1
      if (attempts < 3) throw new RuntimeException("transient")
      attempts
    }
    assert(result == 3)
    assert(delays.toList == List(200L, 400L))
  }

  it should "honor a custom initial delay when computing the progression" in {
    // Guards against a hardcoded base: from 50ms the progression must be 50, 100, 200.
    val delays = ListBuffer.empty[Long]
    intercept[RuntimeException] {
      RetryUtil.withBackoff("reach the store", 4, 50L, noRetryHook, delays += _) {
        throw new RuntimeException("down")
      }
    }
    assert(delays.toList == List(50L, 100L, 200L))
  }

  it should "succeed on the final permitted attempt without giving up one try too early" in {
    // Boundary for `attempt >= maxAttempts`: success on the very last attempt must still count.
    val delays = ListBuffer.empty[Long]
    var attempts = 0
    RetryUtil.withBackoff("reach the store", 3, 200L, noRetryHook, delays += _) {
      attempts += 1
      if (attempts < 3) throw new RuntimeException("transient")
    }
    assert(attempts == 3)
    assert(delays.toList == List(200L, 400L))
  }

  it should "give up after maxAttempts, naming the description and preserving the cause" in {
    val cause = new RuntimeException("still down")
    var attempts = 0
    val failure = intercept[RuntimeException] {
      RetryUtil.withBackoff("connect to lake fs server", 3, 200L, noRetryHook, _ => ()) {
        attempts += 1
        throw cause
      }
    }
    assert(attempts == 3)
    assert(failure.getMessage == "Failed to connect to lake fs server after 3 attempts: still down")
    assert(failure.getCause eq cause)
  }

  it should "give up immediately without sleeping when maxAttempts is one" in {
    val delays = ListBuffer.empty[Long]
    var attempts = 0
    val failure = intercept[RuntimeException] {
      RetryUtil.withBackoff("reach the store", 1, 200L, noRetryHook, delays += _) {
        attempts += 1
        throw new RuntimeException("still down")
      }
    }
    assert(attempts == 1)
    assert(delays.isEmpty)
    assert(failure.getMessage.contains("after 1 attempts"))
  }

  it should "report each retry to the hook, and never after the last attempt" in {
    val observed = ListBuffer.empty[RetryAttempt]
    intercept[RuntimeException] {
      RetryUtil.withBackoff("reach the store", 3, 200L, observed += _, _ => ()) {
        throw new RuntimeException("down")
      }
    }
    // 3 attempts buy 2 retries, so the hook fires twice with 1-based attempt numbers.
    assert(
      observed.map(a => (a.attempt, a.maxAttempts, a.delayMillis)).toList == List(
        (1, 3, 200L),
        (2, 3, 400L)
      )
    )
    assert(observed.forall(_.cause.getMessage == "down"))
    assert(
      observed.head.message ==
        "Failed to reach the store (attempt 1/3): down. Retrying in 200ms..."
    )
  }

  it should "fail fast and restore the interrupt status when the operation is interrupted" in {
    val delays = ListBuffer.empty[Long]
    val failure = intercept[RuntimeException] {
      RetryUtil.withBackoff("reach the store", 5, 200L, noRetryHook, delays += _) {
        throw new InterruptedException("interrupted")
      }
    }
    // Thread.interrupted() both reads and clears the flag, so the interrupt was restored.
    assert(Thread.interrupted())
    assert(failure.getMessage == "Interrupted while waiting to reach the store")
    assert(failure.getCause.isInstanceOf[InterruptedException])
    assert(delays.isEmpty)
  }

  it should "fail fast and restore the interrupt status when interrupted during a backoff sleep" in {
    // The hole in the LakeFS copy: its `sleep` sat inside the `catch`, so an interrupt raised
    // while waiting escaped raw, with the interrupt flag left cleared.
    var attempts = 0
    val failure = intercept[RuntimeException] {
      RetryUtil.withBackoff(
        "reach the store",
        5,
        200L,
        noRetryHook,
        _ => throw new InterruptedException("interrupted")
      ) {
        attempts += 1
        throw new RuntimeException("transient")
      }
    }
    assert(attempts == 1)
    assert(Thread.interrupted())
    assert(failure.getMessage == "Interrupted while waiting to reach the store")
    assert(failure.getCause.isInstanceOf[InterruptedException])
  }

  it should "not retry a fatal throwable, and let it through unwrapped" in {
    // Only `NonFatal` failures are transient. A control throwable stands in for a fatal here
    // because a real `VirtualMachineError` would abort the suite rather than be caught.
    val delays = ListBuffer.empty[Long]
    var attempts = 0
    object Fatal extends ControlThrowable
    intercept[ControlThrowable] {
      RetryUtil.withBackoff("reach the store", 5, 200L, noRetryHook, delays += _) {
        attempts += 1
        throw Fatal
      }
    }
    assert(attempts == 1)
    assert(delays.isEmpty)
  }
}
