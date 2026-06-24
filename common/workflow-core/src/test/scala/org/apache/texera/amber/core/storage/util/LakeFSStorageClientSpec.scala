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

package org.apache.texera.amber.core.storage.util

import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ListBuffer

class LakeFSStorageClientSpec extends AnyFlatSpec {

  "retryWithBackoff" should "run the operation once and not sleep when it succeeds immediately" in {
    var attempts = 0
    val delays = ListBuffer.empty[Long]
    LakeFSStorageClient.retryWithBackoff(5, 200L, delays += _) {
      attempts += 1
    }
    assert(attempts == 1)
    assert(delays.isEmpty)
  }

  it should "retry until success and double the delay after each failed attempt" in {
    var attempts = 0
    val delays = ListBuffer.empty[Long]
    LakeFSStorageClient.retryWithBackoff(5, 200L, delays += _) {
      attempts += 1
      if (attempts < 3) throw new RuntimeException("transient")
    }
    assert(attempts == 3)
    assert(delays.toList == List(200L, 400L))
  }

  it should "give up after maxAttempts and preserve the last failure as the cause" in {
    var attempts = 0
    val cause = new RuntimeException("still down")
    val ex = intercept[RuntimeException] {
      LakeFSStorageClient.retryWithBackoff(3, 200L, _ => ()) {
        attempts += 1
        throw cause
      }
    }
    assert(attempts == 3)
    assert(ex.getMessage.contains("after 3 attempts"))
    assert(ex.getCause eq cause)
  }

  it should "fail fast and restore the interrupt status when interrupted" in {
    val ex = intercept[RuntimeException] {
      LakeFSStorageClient.retryWithBackoff(5, 200L, _ => ()) {
        throw new InterruptedException("interrupted")
      }
    }
    // Thread.interrupted() both reads and clears the flag, so the interrupt was restored.
    assert(Thread.interrupted())
    assert(ex.getCause.isInstanceOf[InterruptedException])
  }
}
