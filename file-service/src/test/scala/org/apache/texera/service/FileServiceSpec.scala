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

import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ListBuffer

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
}
