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

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.core.AppenderBase
import com.twitter.util.Future
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  ControlRequest,
  DebugCommandRequest,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  ControlError,
  ControlReturn,
  IntResponse,
  ReturnInvocation
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
  * Plain-JVM unit test for [[AsyncRPCServer]]: no actor system, no DB, no network.
  * The server is driven through its public surface only -- the `handler` var plus
  * `receive` -- with a [[NetworkOutputGateway]] whose send handler captures the
  * emitted messages into an in-memory buffer (the same fixture shape
  * `AsyncRPCClientSpec` uses).
  *
  * Every test builds a FRESH server: `AsyncRPCServer.methodsByName` is a memoized
  * `@transient lazy val` built from `handler.getClass.getMethods` on the first
  * `receive`, so re-assigning `handler` on a server that has already dispatched
  * once is silently ignored and would make later tests vacuous.
  */
class AsyncRPCServerSpec extends AnyFlatSpec with Matchers {

  private val serverId = ActorVirtualIdentity("rpc-server")
  private val senderId = ActorVirtualIdentity("rpc-sender")

  // Four DISTINCT identities on purpose. The context that travels inside the
  // invocation carries its own two, so neither of them can stand in for the wire
  // sender or for the server's own id: production is free to route the reply to
  // `context.sender` instead of to the actor the message actually came from, and a
  // fixture that reused one literal for both could not see the difference.
  private val contextOrigin = ActorVirtualIdentity("rpc-context-origin")
  private val contextTarget = ActorVirtualIdentity("rpc-context-target")
  private val context = AsyncRPCContext(contextOrigin, contextTarget)

  /** Fresh server, its handler stub, and the buffer its output gateway writes into. */
  private def newFixture(
      actorId: ActorVirtualIdentity = serverId
  ): (AsyncRPCServer, AsyncRPCServerSpecHandler, ArrayBuffer[WorkflowFIFOMessage]) = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val out = new NetworkOutputGateway(actorId, msg => { sent += msg; () })
    val server = new AsyncRPCServer(out, actorId)
    val handler = new AsyncRPCServerSpecHandler
    server.handler = handler
    (server, handler, sent)
  }

  private def invocation(
      methodName: String,
      commandId: Long,
      request: ControlRequest = EmptyRequest()
  ): ControlInvocation = ControlInvocation(methodName, request, context, commandId)

  /** The single control message the server replied with; fails loudly if it sent 0 or 2+. */
  private def soleReturn(sent: ArrayBuffer[WorkflowFIFOMessage]): ReturnInvocation = {
    sent should have size 1
    val msg = sent.head
    msg.channelId shouldBe ChannelIdentity(serverId, senderId, isControl = true)
    msg.payload shouldBe a[ReturnInvocation]
    msg.payload.asInstanceOf[ReturnInvocation]
  }

  // ---------------------------------------------------------------------------
  // dispatch
  // ---------------------------------------------------------------------------

  "receive" should "dispatch a known method case-insensitively and return its value under the request's commandId" in {
    val (server, handler, sent) = newFixture()
    // A request with FIELDS, not the default EmptyRequest: two EmptyRequests are
    // equal, so an EmptyRequest here would be satisfied by a production that threw
    // the wire request away and handed the handler one it invented.
    val request = DebugCommandRequest("worker-9", "print stats")

    // The lookup map is keyed on the lower-cased name, so the mixed-case
    // methodName on the wire must still resolve.
    server.receive(invocation("EchoNumber", 7L, request), senderId)

    // The handler saw the request and the context from the invocation -- not, say,
    // the two swapped, or the context rebuilt from the sender id.
    handler.lastRequest shouldBe request
    handler.lastContext shouldBe context
    soleReturn(sent) shouldBe ReturnInvocation(7L, IntResponse(42))
  }

  it should "answer a failed handler future with a ControlError naming the failure" in {
    val (server, _, sent) = newFixture()

    server.receive(invocation("failingFuture", 11L), senderId)

    val ret = soleReturn(sent)
    ret.commandId shouldBe 11L
    ret.returnValue shouldBe a[ControlError]
    ret.returnValue.asInstanceOf[ControlError].errorMessage should include("future rejected")
  }

  it should "unwrap the cause when the handler throws synchronously, rather than reporting the reflective wrapper" in {
    val (server, _, sent) = newFixture()

    // A synchronous throw inside the handler reaches the server wrapped in an
    // InvocationTargetException; production rethrows `e.getCause` so the sender
    // sees the real failure.
    server.receive(invocation("throwingCall", 12L), senderId)

    val ret = soleReturn(sent)
    ret.commandId shouldBe 12L
    val error = ret.returnValue.asInstanceOf[ControlError]
    error.errorMessage should include("handler exploded")
    error.errorMessage should not include "InvocationTargetException"
  }

  it should "log an error naming the unknown method and dispatch nothing when no handler method matches" in {
    val actorId = ActorVirtualIdentity("rpc-unknown-method")
    withCapturedLogs(actorId, Level.ERROR) { events =>
      val sent = ArrayBuffer[WorkflowFIFOMessage]()
      val out = new NetworkOutputGateway(actorId, msg => { sent += msg; () })
      val server = new AsyncRPCServer(out, actorId)
      val handler = new AsyncRPCServerSpecHandler
      server.handler = handler

      server.receive(invocation("noSuchMethod", 13L), senderId)

      // Nothing ran: an unresolvable name must not fall through onto some other
      // entry of the lookup map.
      handler.invocationCount shouldBe 0

      // DELIBERATELY NOT asserting on `sent`. Production sends nothing at all from
      // this arm -- it only logs -- so the caller's promise for this commandId is
      // never resolved, and a mistyped or version-skewed method name surfaces as a
      // hang on the sender's own timeout instead of as a ControlError. That is a
      // defect in AsyncRPCServer, reported rather than pinned here: an assertion
      // that the server stays silent would fail the day somebody fixes it.
      val errors = events().filter(_.getLevel == Level.ERROR).map(_.getFormattedMessage)
      errors should contain("No methods found with name nosuchmethod")
    }
  }

  it should "emit the command trace naming the method and the sender when debug logging is enabled" in {
    val actorId = ActorVirtualIdentity("rpc-debug-trace")
    withCapturedLogs(actorId, Level.DEBUG) { events =>
      val (server, _, sent) = newFixture(actorId)

      server.receive(invocation("echoNumber", 14L), senderId)

      sent should have size 1
      val debugs = events().filter(_.getLevel == Level.DEBUG).map(_.getFormattedMessage)
      debugs.filter(_.startsWith("receive command: echonumber ")) match {
        case Seq(only) =>
          only should include(s"from $senderId")
          only should include("(controlID: 14)")
        case other =>
          fail(s"expected exactly one command trace for echonumber, got: $other")
      }
    }
  }

  it should "dispatch but send no reply when the commandId is negative" in {
    val (server, handler, sent) = newFixture()

    // Negative ids are the fire-and-forget convention (AsyncRPCClient's
    // IgnoreReplyAndDoNotLog is -2): the call still runs, the result is dropped.
    server.receive(invocation("echoNumber", -2L), senderId)

    handler.invocationCount shouldBe 1
    sent shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // logging fixture
  // ---------------------------------------------------------------------------

  /** Buffers the events a logger emits; appends may arrive from any thread. */
  private final class CollectingAppender extends AppenderBase[ILoggingEvent] {
    private val events = new ConcurrentLinkedQueue[ILoggingEvent]

    override def append(event: ILoggingEvent): Unit = events.add(event)

    def snapshot: Seq[ILoggingEvent] = events.asScala.toSeq
  }

  /**
    * Runs `body` with the [[AsyncRPCServer]] logger for `actorId` pinned to `level`
    * and a capturing appender attached, handing it a live view of what was logged.
    *
    * `AmberLogging` names the logger after the actor id, so a distinctive id keeps
    * the level change isolated from every other suite -- which matters because
    * amber's suites share one JVM and run strictly serially. The previous level and
    * additivity are restored in a `finally`; additivity is switched off meanwhile so
    * the captured lines do not also reach the console and rolling-file appenders.
    */
  private def withCapturedLogs[T](actorId: ActorVirtualIdentity, level: Level)(
      body: (() => Seq[ILoggingEvent]) => T
  ): T = {
    val loggerName = s"${VirtualIdentityUtils.toShorterString(actorId)}] [AsyncRPCServer"
    val logger = LoggerFactory.getLogger(loggerName).asInstanceOf[LogbackLogger]
    val appender = new CollectingAppender
    val previousLevel = logger.getLevel
    val previousAdditive = logger.isAdditive
    appender.setContext(logger.getLoggerContext)
    appender.setName(s"async-rpc-server-spec-appender-${actorId.name}")
    appender.start()
    logger.addAppender(appender)
    logger.setLevel(level)
    logger.setAdditive(false)
    try body(() => appender.snapshot)
    finally {
      logger.detachAppender(appender)
      logger.setAdditive(previousAdditive)
      logger.setLevel(previousLevel)
      appender.stop()
    }
  }
}

/**
  * Handler stub for [[AsyncRPCServerSpec]].
  *
  * Deliberately a top-level, public class. `AsyncRPCServer.methodsByName` is built
  * from `handler.getClass.getMethods` and dispatch goes through `Method.invoke`, so
  * a handler emitted with package-private access would fail every dispatch with an
  * IllegalAccessException and route the happy-path test into the same error branch
  * as the failure tests, making both vacuous.
  */
class AsyncRPCServerSpecHandler {

  var lastRequest: ControlRequest = _
  var lastContext: AsyncRPCContext = _
  var invocationCount: Int = 0

  def echoNumber(request: ControlRequest, ctx: AsyncRPCContext): Future[ControlReturn] = {
    lastRequest = request
    lastContext = ctx
    invocationCount += 1
    Future.value(IntResponse(42))
  }

  def failingFuture(request: ControlRequest, ctx: AsyncRPCContext): Future[ControlReturn] =
    Future.exception(new IllegalStateException("future rejected"))

  def throwingCall(request: ControlRequest, ctx: AsyncRPCContext): Future[ControlReturn] =
    throw new IllegalStateException("handler exploded")
}
