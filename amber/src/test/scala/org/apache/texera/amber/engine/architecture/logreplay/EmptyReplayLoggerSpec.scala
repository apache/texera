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

package org.apache.texera.amber.engine.architecture.logreplay

import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.scalatest.flatspec.AnyFlatSpec

class EmptyReplayLoggerSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private val channelId: ChannelIdentity =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)
  private val ecmId: EmbeddedControlMessageIdentity = EmbeddedControlMessageIdentity("test-ecm")

  // ---------------------------------------------------------------------------
  // drainCurrentLogRecords — always empty
  // ---------------------------------------------------------------------------

  "EmptyReplayLogger.drainCurrentLogRecords" should
    "return an empty Array[ReplayLogRecord] regardless of the step argument" in {
    val logger = new EmptyReplayLogger
    val r0 = logger.drainCurrentLogRecords(0L)
    val r1 = logger.drainCurrentLogRecords(1L)
    val rMax = logger.drainCurrentLogRecords(Long.MaxValue)
    val rNeg = logger.drainCurrentLogRecords(-1L)
    assert(r0.isEmpty)
    assert(r1.isEmpty)
    assert(rMax.isEmpty)
    assert(rNeg.isEmpty)
  }

  it should "return a non-null array (callers iterate it without null-checking)" in {
    val logger = new EmptyReplayLogger
    val r = logger.drainCurrentLogRecords(42L)
    assert(r != null)
    assert(r.length == 0)
  }

  it should "return arrays whose element type is ReplayLogRecord (compile-time enforced)" in {
    // If a future refactor accidentally widened the return type to
    // `Array[AnyRef]`, this would fail to typecheck. Pin the contract.
    val logger = new EmptyReplayLogger
    val r: Array[ReplayLogRecord] = logger.drainCurrentLogRecords(0L)
    assert(r.length == 0)
  }

  // ---------------------------------------------------------------------------
  // markAsReplayDestination — no-op
  // ---------------------------------------------------------------------------

  "EmptyReplayLogger.markAsReplayDestination" should
    "accept any EmbeddedControlMessageIdentity without throwing" in {
    val logger = new EmptyReplayLogger
    logger.markAsReplayDestination(ecmId) // must not throw
    // Calling twice with the same id is still a no-op.
    logger.markAsReplayDestination(ecmId)
    succeed
  }

  it should "leave drainCurrentLogRecords output untouched (no internal buffer accumulates)" in {
    val logger = new EmptyReplayLogger
    logger.markAsReplayDestination(ecmId)
    logger.markAsReplayDestination(EmbeddedControlMessageIdentity("another"))
    assert(logger.drainCurrentLogRecords(0L).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // logCurrentStepWithMessage — no-op
  // ---------------------------------------------------------------------------

  "EmptyReplayLogger.logCurrentStepWithMessage" should
    "accept any (step, channelId, msg) triple without throwing" in {
    val logger = new EmptyReplayLogger
    logger.logCurrentStepWithMessage(0L, channelId, msg = None)
    logger.logCurrentStepWithMessage(1L, channelId, msg = None)
    logger.logCurrentStepWithMessage(Long.MaxValue, channelId, msg = None)
    succeed
  }

  it should "tolerate a None msg argument (the null-object's job is to absorb every call)" in {
    val logger = new EmptyReplayLogger
    logger.logCurrentStepWithMessage(7L, channelId, msg = None)
    // Verify nothing was queued in the process.
    assert(logger.drainCurrentLogRecords(7L).isEmpty)
  }

  it should "leave drainCurrentLogRecords output empty even after many calls" in {
    val logger = new EmptyReplayLogger
    (1L to 100L).foreach(i => logger.logCurrentStepWithMessage(i, channelId, msg = None))
    assert(logger.drainCurrentLogRecords(100L).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // ReplayLogger trait conformance
  // ---------------------------------------------------------------------------
  //
  // The null-object pattern requires EmptyReplayLogger to be a drop-in for
  // ReplayLogger callers — pin the upcast.

  "EmptyReplayLogger" should "be usable through the ReplayLogger interface" in {
    val logger: ReplayLogger = new EmptyReplayLogger
    logger.logCurrentStepWithMessage(0L, channelId, msg = None)
    logger.markAsReplayDestination(ecmId)
    assert(logger.drainCurrentLogRecords(0L).isEmpty)
  }
}
