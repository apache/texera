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

package org.apache.texera.amber.engine.common.rpc

import com.twitter.util.{Await, Duration}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.coordinator.ExecutionStateUpdate
import org.apache.texera.amber.engine.architecture.messaginglayer.{
  NetworkInputGateway,
  NetworkOutputGateway
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmptyRequest
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  ControlError,
  EmptyReturn,
  IntResponse,
  ReturnInvocation,
  WorkerMetricsResponse,
  WorkflowAggregatedState
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.CLIENT
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

/**
  * Plain-JVM unit test for [[AsyncRPCClient]]. No actor system, DB, or network:
  * collaborators are built directly and the output gateway's handler captures
  * emitted messages into an in-memory buffer.
  */
class AsyncRPCClientSpec extends AnyFlatSpec with Matchers {

  private val actorId = ActorVirtualIdentity("self")
  private val other = ActorVirtualIdentity("other")

  // A control channel the log path can attribute replies to.
  private val channelId = ChannelIdentity(other, actorId, isControl = true)

  private val awaitTimeout: Duration = Duration.fromSeconds(5)

  /**
    * Fresh client plus the buffer that its output gateway writes into. Each test
    * gets an isolated instance so the monotonic commandID counter and the
    * captured-message buffer don't leak across tests.
    */
  private def newFixture(): (AsyncRPCClient, ArrayBuffer[WorkflowFIFOMessage]) = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val out = new NetworkOutputGateway(actorId, msg => { sent += msg; () })
    val in = new NetworkInputGateway(actorId)
    val client = new AsyncRPCClient(in, out, actorId)
    (client, sent)
  }

  // ---------------------------------------------------------------------------
  // createInvocation + fulfillPromise
  // ---------------------------------------------------------------------------

  "createInvocation" should "carry methodName/context and expose an unfulfilled future" in {
    val (client, _) = newFixture()
    val context = client.mkContext(other)
    val (invocation, future) = client.createInvocation("myMethod", EmptyRequest(), context)

    assert(invocation.methodName == "myMethod")
    assert(invocation.command == EmptyRequest())
    assert(invocation.context == context)
    // The promise starts unresolved.
    assert(!future.isDefined)
  }

  it should "assign monotonically increasing commandIDs (1 then 2)" in {
    val (client, _) = newFixture()
    val context = client.mkContext(other)
    val (first, _) = client.createInvocation("first", EmptyRequest(), context)
    val (second, _) = client.createInvocation("second", EmptyRequest(), context)

    assert(first.commandId == 1L)
    assert(second.commandId == 2L)
  }

  it should "complete the returned future when the matching promise is fulfilled" in {
    val (client, _) = newFixture()
    val context = client.mkContext(other)
    val (invocation, future) = client.createInvocation("resolvable", EmptyRequest(), context)

    val response = IntResponse(7)
    client.fulfillPromise(ReturnInvocation(invocation.commandId, response))

    assert(future.isDefined)
    assert(Await.result(future, awaitTimeout) == response)
  }

  it should "fail the returned future with a reconstructed throwable on a ControlError return" in {
    val (client, _) = newFixture()
    val context = client.mkContext(other)
    val (invocation, future) = client.createInvocation("failing", EmptyRequest(), context)

    client.fulfillPromise(ReturnInvocation(invocation.commandId, ControlError.defaultInstance))

    assert(future.isDefined)
    assertThrows[Throwable] {
      Await.result(future, awaitTimeout)
    }
  }

  it should "ignore a ReturnInvocation whose commandID has no pending promise" in {
    val (client, _) = newFixture()
    // No createInvocation call, so commandID 999 is unknown; this must be a no-op.
    noException should be thrownBy {
      client.fulfillPromise(ReturnInvocation(999L, EmptyReturn()))
    }
  }

  // ---------------------------------------------------------------------------
  // logControlReply — the four (plus null) branches
  // ---------------------------------------------------------------------------

  "logControlReply" should "early-return for IgnoreReplyAndDoNotLog without throwing" in {
    val (client, _) = newFixture()
    noException should be thrownBy {
      client.logControlReply(
        ReturnInvocation(AsyncRPCClient.IgnoreReplyAndDoNotLog.toLong, EmptyReturn()),
        channelId
      )
    }
  }

  it should "early-return for a WorkerMetricsResponse return without throwing" in {
    val (client, _) = newFixture()
    noException should be thrownBy {
      client.logControlReply(
        ReturnInvocation(1L, WorkerMetricsResponse.defaultInstance),
        channelId
      )
    }
  }

  it should "take the debug path for a normal non-error return without throwing" in {
    val (client, _) = newFixture()
    noException should be thrownBy {
      client.logControlReply(ReturnInvocation(2L, EmptyReturn()), channelId)
    }
  }

  it should "take the error-log path for a ControlError return without throwing" in {
    val (client, _) = newFixture()
    noException should be thrownBy {
      client.logControlReply(ReturnInvocation(3L, ControlError.defaultInstance), channelId)
    }
  }

  it should "take the info-log else branch for a null return value without throwing" in {
    val (client, _) = newFixture()
    // returnValue is a non-boxed sealed oneof; passing null exercises the
    // `ret.returnValue != null` else branch that logs "null".
    noException should be thrownBy {
      client.logControlReply(ReturnInvocation(4L, null), channelId)
    }
  }

  // ---------------------------------------------------------------------------
  // sendToClient
  // ---------------------------------------------------------------------------

  "sendToClient" should "forward exactly one message on the client control channel" in {
    val (client, sent) = newFixture()
    val before = sent.size

    client.sendToClient(ExecutionStateUpdate(WorkflowAggregatedState.RUNNING))

    assert(sent.size == before + 1)
    val msg = sent.last
    assert(msg.channelId.fromWorkerId == actorId)
    assert(msg.channelId.toWorkerId == CLIENT)
    assert(msg.channelId.isControl)
    assert(msg.payload == ExecutionStateUpdate(WorkflowAggregatedState.RUNNING))
  }
}
