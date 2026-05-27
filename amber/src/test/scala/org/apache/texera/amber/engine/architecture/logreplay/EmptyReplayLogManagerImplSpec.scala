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
import org.apache.texera.amber.engine.architecture.common.ProcessingStepCursor
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import org.apache.texera.amber.engine.common.storage.EmptyRecordStorage
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class EmptyReplayLogManagerImplSpec extends AnyFlatSpec {

  private val channel =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  private def fifo(
      seq: Long,
      payload: WorkflowFIFOMessagePayload = DataFrame(Array.empty)
  ): WorkflowFIFOMessage =
    WorkflowFIFOMessage(channel, seq, payload)

  private class CapturingHandler {
    val received: mutable.ListBuffer[Either[MainThreadDelegateMessage, WorkflowFIFOMessage]] =
      mutable.ListBuffer()
    val handler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit =
      msg => received += msg
  }

  "EmptyReplayLogManagerImpl" should "expose getStep starting at INIT_STEP" in {
    val mgr = new EmptyReplayLogManagerImpl(_ => ())
    assert(mgr.getStep == ProcessingStepCursor.INIT_STEP)
  }

  it should "no-op on setupWriter / markAsReplayDestination / terminate" in {
    val mgr = new EmptyReplayLogManagerImpl(_ => ())
    // Use real fixtures rather than nulls so the test reflects realistic
    // call sites and would catch an accidental NPE if the no-op shape ever
    // changes.
    val writer = new EmptyRecordStorage[ReplayLogRecord]().getWriter("x")
    mgr.setupWriter(writer)
    mgr.markAsReplayDestination(EmbeddedControlMessageIdentity("test"))
    mgr.terminate()
    assert(mgr.getStep == ProcessingStepCursor.INIT_STEP)
  }

  "EmptyReplayLogManagerImpl.sendCommitted" should "forward the message to the configured handler" in {
    val cap = new CapturingHandler
    val mgr = new EmptyReplayLogManagerImpl(cap.handler)
    val msg = Right[MainThreadDelegateMessage, WorkflowFIFOMessage](fifo(1L))
    mgr.sendCommitted(msg)
    assert(cap.received.toList == List(msg))
  }

  "ReplayLogManager.withFaultTolerant" should "advance the step counter after the body runs" in {
    val mgr = new EmptyReplayLogManagerImpl(_ => ())
    // Express the expected step relative to INIT_STEP so the test does not
    // need to be touched if the initial-step constant ever changes.
    mgr.withFaultTolerant(channel, Some(fifo(1L))) {}
    assert(mgr.getStep == ProcessingStepCursor.INIT_STEP + 1)
    mgr.withFaultTolerant(channel, Some(fifo(2L))) {}
    assert(mgr.getStep == ProcessingStepCursor.INIT_STEP + 2)
  }

  it should "still advance the step counter and rethrow when the body throws" in {
    val mgr = new EmptyReplayLogManagerImpl(_ => ())
    intercept[RuntimeException] {
      mgr.withFaultTolerant(channel, Some(fifo(1L))) {
        throw new RuntimeException("boom")
      }
    }
    assert(mgr.getStep == ProcessingStepCursor.INIT_STEP + 1)
  }
}
