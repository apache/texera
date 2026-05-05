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

package org.apache.texera.amber.engine.architecture.controller

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.scalatest.flatspec.AnyFlatSpec

class GlobalReplayManagerSpec extends AnyFlatSpec {

  private class CallbackCounter {
    var startCount = 0
    var completeCount = 0
    val onStart: () => Unit = () => startCount += 1
    val onComplete: () => Unit = () => completeCount += 1
  }

  private val workerA = ActorVirtualIdentity("a")
  private val workerB = ActorVirtualIdentity("b")

  "GlobalReplayManager" should "fire onRecoveryStart on the first transition into recovery" in {
    val cb = new CallbackCounter
    val mgr = new GlobalReplayManager(cb.onStart, cb.onComplete)

    mgr.markRecoveryStatus(workerA, isRecovering = true)
    assert(cb.startCount == 1)
    assert(cb.completeCount == 0)
  }

  it should "not refire onRecoveryStart while still recovering" in {
    val cb = new CallbackCounter
    val mgr = new GlobalReplayManager(cb.onStart, cb.onComplete)

    mgr.markRecoveryStatus(workerA, isRecovering = true)
    mgr.markRecoveryStatus(workerB, isRecovering = true)
    assert(cb.startCount == 1, "onStart must fire only on the first transition into recovery")
  }

  it should "fire onRecoveryComplete only once all recovering workers have cleared" in {
    val cb = new CallbackCounter
    val mgr = new GlobalReplayManager(cb.onStart, cb.onComplete)

    mgr.markRecoveryStatus(workerA, isRecovering = true)
    mgr.markRecoveryStatus(workerB, isRecovering = true)
    mgr.markRecoveryStatus(workerA, isRecovering = false)
    assert(cb.completeCount == 0, "still has recovering workers")
    mgr.markRecoveryStatus(workerB, isRecovering = false)
    assert(cb.completeCount == 1)
  }

  it should "not fire onRecoveryComplete when no recovery was ever started" in {
    val cb = new CallbackCounter
    val mgr = new GlobalReplayManager(cb.onStart, cb.onComplete)

    mgr.markRecoveryStatus(workerA, isRecovering = false)
    assert(cb.startCount == 0)
    assert(cb.completeCount == 0)
  }

  it should "be idempotent for repeated isRecovering=true on the same worker" in {
    val cb = new CallbackCounter
    val mgr = new GlobalReplayManager(cb.onStart, cb.onComplete)

    mgr.markRecoveryStatus(workerA, isRecovering = true)
    mgr.markRecoveryStatus(workerA, isRecovering = true)
    mgr.markRecoveryStatus(workerA, isRecovering = false)
    assert(cb.startCount == 1)
    assert(cb.completeCount == 1)
  }

  it should "fire onRecoveryStart again when recovery restarts after completing" in {
    val cb = new CallbackCounter
    val mgr = new GlobalReplayManager(cb.onStart, cb.onComplete)

    // First cycle: start and finish.
    mgr.markRecoveryStatus(workerA, isRecovering = true)
    mgr.markRecoveryStatus(workerA, isRecovering = false)
    assert(cb.startCount == 1)
    assert(cb.completeCount == 1)

    // Second cycle: a new transition into recovery must fire onStart again,
    // and the subsequent clear must fire onComplete again.
    mgr.markRecoveryStatus(workerB, isRecovering = true)
    mgr.markRecoveryStatus(workerB, isRecovering = false)
    assert(cb.startCount == 2)
    assert(cb.completeCount == 2)
  }
}
