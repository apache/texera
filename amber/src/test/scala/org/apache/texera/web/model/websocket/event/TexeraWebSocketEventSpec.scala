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

package org.apache.texera.web.model.websocket.event

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.google.protobuf.timestamp.Timestamp
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.core.workflowruntimestate.{FatalErrorType, WorkflowFatalError}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  ConsoleMessage,
  ConsoleMessageType
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{EvaluatedValue, TypedValue}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.web.model.websocket.event.python.ConsoleUpdateEvent
import org.apache.texera.web.model.websocket.response.python.PythonExpressionEvaluateResponse
import org.apache.texera.web.model.websocket.response.{
  ClusterStatusUpdateEvent,
  HeartBeatResponse,
  ModifyLogicCompletedEvent,
  ModifyLogicResponse,
  RegionUpdateEvent
}
import org.apache.texera.web.service.ExecutionResultService.{
  PaginationMode,
  SetDeltaMode,
  WebDataUpdate,
  WebPaginationUpdate
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/**
  * Pins the server -> client half of the websocket wire contract.
  *
  * `SessionState.send` ships every event with
  * `session.getAsyncRemote.sendText(objectMapper.writeValueAsString(msg))`, where
  * `msg: TexeraWebSocketEvent` and the mapper is `JSONUtils.objectMapper`. This spec
  * uses that exact mapper and that exact call so the emitted bytes are the real ones.
  *
  * `TexeraWebSocketEvent` carries
  * `@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "type")` and none of
  * its `@JsonSubTypes.Type` entries supply `name =`, so the wire id is Jackson's
  * `TypeNameIdResolver` default: the BARE SIMPLE CLASS NAME. The Angular client
  * switches on those literals; `frontend/src/app/workspace/types/workflow-websocket.interface.ts`
  * spells the rule out:
  *
  *   "Each type definition MUST follow the following rules:
  *    in either TexeraWebsocketRequestTypeMap or TexeraWebsocketEventTypeMap
  *    add a map entry:
  *    1. key is the 'type' string, it must be the same as corresponding backend class name
  *    2. value is the payload this request/event needs"
  *
  * Its `TexeraWebsocketEventTypeMap` keys include `HeartBeatResponse`,
  * `WorkflowStateEvent`, `OperatorStatisticsUpdateEvent`, `WebResultUpdateEvent`,
  * `WorkflowErrorEvent`, `ConsoleUpdateEvent`, `PaginatedResultEvent`,
  * `CacheStatusUpdateEvent`, `PythonExpressionEvaluateResponse`,
  * `WorkerAssignmentUpdateEvent`, `ModifyLogicResponse`, `ModifyLogicCompletedEvent`,
  * `ExecutionDurationUpdateEvent`, `ClusterStatusUpdateEvent`, `RegionUpdateEvent` and
  * `RegionStateEvent`. Renaming a Scala event class compiles fine on both sides and
  * silently drops the corresponding UI update -- that is what this spec guards.
  *
  * Deliberate asymmetry: events are tested SERIALIZE-first. Five live event classes
  * (`ExecutionDurationUpdateEvent`, `RegionStateEvent`, `ModifyLogicCompletedEvent`,
  * `ClusterStatusUpdateEvent`, `RegionUpdateEvent`) are NOT listed in `@JsonSubTypes`.
  * They serialize correctly through the simple-name fallback, but `typeFromId` cannot
  * resolve them, so a blanket read-back loop would throw `InvalidTypeIdException`.
  * That is harmless today because those events are outbound only -- and it is pinned
  * below so nobody assumes the tier is symmetric.
  */
class TexeraWebSocketEventSpec extends AnyFlatSpec with Matchers {

  private def write(e: TexeraWebSocketEvent): String = objectMapper.writeValueAsString(e)

  private def typeIdOf(e: TexeraWebSocketEvent): String =
    objectMapper.readTree(write(e)).get("type").asText()

  private val fatalError = WorkflowFatalError(
    FatalErrorType.EXECUTION_FAILURE,
    Timestamp(Instant.ofEpochSecond(1_700_000_000L)),
    "message-1",
    "details-2",
    "op-3",
    "worker-4"
  )

  private val consoleMessage = ConsoleMessage(
    "worker-5",
    Timestamp(Instant.ofEpochSecond(1_700_000_001L)),
    ConsoleMessageType.PRINT,
    "source-6",
    "title-7",
    "message-8"
  )

  private val metrics = OperatorAggregatedMetrics(
    operatorState = "COMPLETED",
    aggregatedInputRowCount = 11L,
    aggregatedInputSize = 12L,
    inputPortMetrics = Map("in-0" -> 13L),
    aggregatedOutputRowCount = 14L,
    aggregatedOutputSize = 15L,
    outputPortMetrics = Map("out-0" -> 16L),
    numWorkers = 17L,
    aggregatedDataProcessingTime = 18L,
    aggregatedControlProcessingTime = 19L,
    aggregatedIdleTime = 20L,
    // Non-default on purpose: the symmetric round trip below only pins this
    // field on the wire if a drop would change the value read back.
    reusedFromCache = true
  )

  private val resultRow = objectMapper.createObjectNode().put("city", "Irvine")

  /** The 11 events that ARE in `@JsonSubTypes`, one instance each. */
  private val registeredEvents: List[(String, TexeraWebSocketEvent)] = List(
    "HeartBeatResponse" -> HeartBeatResponse(),
    "WorkflowErrorEvent" -> WorkflowErrorEvent(Seq(fatalError)),
    "WorkflowStateEvent" -> WorkflowStateEvent("Running"),
    "OperatorStatisticsUpdateEvent" -> OperatorStatisticsUpdateEvent(Map("op-stats" -> metrics)),
    "WebResultUpdateEvent" -> WebResultUpdateEvent(
      Map("op-page" -> WebPaginationUpdate(PaginationMode(), 7L, List(1, 3))),
      Map("op-page" -> Map("city" -> Map[String, Any]("distinct" -> 2)))
    ),
    "ConsoleUpdateEvent" -> ConsoleUpdateEvent("op-console", Seq(consoleMessage)),
    "CacheStatusUpdateEvent" -> CacheStatusUpdateEvent(Map("op-cache" -> "cache valid")),
    "PaginatedResultEvent" -> PaginatedResultEvent(
      "req-1",
      "op-2",
      3,
      List(resultRow),
      List(new Attribute("city", AttributeType.STRING))
    ),
    "PythonExpressionEvaluateResponse" -> PythonExpressionEvaluateResponse(
      "len(tuple_)",
      Seq(EvaluatedValue(Some(TypedValue("expr-1", "ref-2", "str-3", "type-4", true)), Seq.empty))
    ),
    "WorkerAssignmentUpdateEvent" -> WorkerAssignmentUpdateEvent(
      "op-assign",
      Seq("worker-9", "worker-10")
    ),
    "ModifyLogicResponse" -> ModifyLogicResponse("op-modify", isValid = false, "error-11")
  )

  /**
    * Live events with a real producer that are NOT in `@JsonSubTypes`. The producers are
    * `ExecutionStatsService` (duration), `RegionExecutionManager` (region state),
    * `ExecutionReconfigurationService` (modify-logic completed), `ClusterListener` /
    * `WorkflowWebsocketResource` (cluster status) and `Coordinator` (region update).
    */
  private val outboundOnlyEvents: List[(String, TexeraWebSocketEvent)] = List(
    "ExecutionDurationUpdateEvent" -> ExecutionDurationUpdateEvent(1234L, isRunning = true),
    "RegionStateEvent" -> RegionStateEvent(21L, "RUNNING"),
    "ModifyLogicCompletedEvent" -> ModifyLogicCompletedEvent(List("op-22")),
    "ClusterStatusUpdateEvent" -> ClusterStatusUpdateEvent(23),
    "RegionUpdateEvent" -> RegionUpdateEvent(List((24L, List("op-25"))))
  )

  // The backend's registered set, spelled out rather than derived so a class rename
  // surfaces as a set diff. Deliberately not phrased as "what the client resolves":
  // the frontend's `TexeraWebsocketEventTypeMap` is a superset — it also keys on the
  // outbound-only events below (which reach it fine via the simple-name fallback)
  // plus a couple with no backend producer at all, e.g. OperatorCurrentTuplesUpdateEvent
  // and RecoveryStartedEvent.
  private val expectedTypeIds: Set[String] = Set(
    "HeartBeatResponse",
    "WorkflowErrorEvent",
    "WorkflowStateEvent",
    "OperatorStatisticsUpdateEvent",
    "WebResultUpdateEvent",
    "ConsoleUpdateEvent",
    "CacheStatusUpdateEvent",
    "PaginatedResultEvent",
    "PythonExpressionEvaluateResponse",
    "WorkerAssignmentUpdateEvent",
    "ModifyLogicResponse"
  )

  "TexeraWebSocketEvent @JsonSubTypes" should
    "register exactly the wire type ids the backend declares" in {
    val subTypes = classOf[TexeraWebSocketEvent].getAnnotation(classOf[JsonSubTypes])
    subTypes should not be null
    subTypes.value().map(_.value().getSimpleName).toSet shouldBe expectedTypeIds
  }

  it should "leave every subtype unnamed so the wire id stays the simple class name" in {
    val named = classOf[TexeraWebSocketEvent]
      .getAnnotation(classOf[JsonSubTypes])
      .value()
      .filter(_.name().nonEmpty)
      .map(t => s"${t.value().getSimpleName}=${t.name()}")
    named.toList shouldBe empty
  }

  "every registered event" should "emit its simple class name as the type property" in {
    registeredEvents.map(_._1).toSet shouldBe expectedTypeIds
    registeredEvents.foreach {
      case (expectedId, event) =>
        withClue(s"${event.getClass.getSimpleName}: ") {
          typeIdOf(event) shouldBe expectedId
        }
    }
  }

  "every outbound-only event" should "still emit a type property via the simple-name fallback" in {
    // Not registered, yet the frontend keys on these ids -- serialization must not
    // silently drop "type" just because the subtype is missing from @JsonSubTypes.
    outboundOnlyEvents.foreach {
      case (expectedId, event) =>
        withClue(s"${event.getClass.getSimpleName}: ") {
          typeIdOf(event) shouldBe expectedId
        }
    }
  }

  // Deliberately NOT asserted: that these five fail to deserialize through the base
  // trait. They do today (InvalidTypeIdException, since @JsonSubTypes has no entry
  // for them), but nothing in main ever reads an event back — the consumer is
  // TypeScript — so pinning the failure would only make a future maintainer's
  // harmless decision to register them turn this suite red.

  "the fully symmetric events" should "survive a write/read round trip" in {
    // Only the events whose payloads are plain Scala/Java values round-trip.
    // WorkflowErrorEvent and ConsoleUpdateEvent cannot: their scalapb enums
    // (FatalErrorType, ConsoleMessageType) have no Jackson creator. WebResultUpdateEvent
    // cannot either: WebResultUpdate is a sealed abstract class with no @JsonTypeInfo.
    val symmetricIds = Set(
      "HeartBeatResponse",
      "WorkflowStateEvent",
      "OperatorStatisticsUpdateEvent",
      "CacheStatusUpdateEvent",
      "PaginatedResultEvent",
      "PythonExpressionEvaluateResponse",
      "WorkerAssignmentUpdateEvent",
      "ModifyLogicResponse"
    )
    val symmetric = registeredEvents.filter { case (id, _) => symmetricIds.contains(id) }
    symmetric.map(_._1).toSet shouldBe symmetricIds
    symmetric.foreach {
      case (id, event) =>
        withClue(s"$id: ") {
          objectMapper.readValue(write(event), classOf[TexeraWebSocketEvent]) shouldBe event
        }
    }
  }

  "WebResultUpdateEvent" should "emit empty maps rather than omitting the keys" in {
    // JSONUtils calls setSerializationInclusion twice (NON_NULL then NON_ABSENT) and
    // the second call replaces the first, so the effective rule is NON_ABSENT — which
    // subsumes NON_NULL but is NOT NON_EMPTY.
    // The Angular WorkflowResultUpdateEvent reads `updates` and `tableStats`
    // unconditionally, so tightening the inclusion rule would break the result panel.
    val json = objectMapper.readTree(write(WebResultUpdateEvent(Map.empty, Map.empty)))
    json.get("type").asText() shouldBe "WebResultUpdateEvent"
    json.has("updates") shouldBe true
    json.get("updates").isObject shouldBe true
    json.get("updates").size() shouldBe 0
    json.has("tableStats") shouldBe true
    json.get("tableStats").size() shouldBe 0
  }

  it should "tag each nested WebResultUpdate mode with its @JsonTypeName" in {
    // WebOutputMode has its own @JsonTypeInfo(Id.NAME) with @JsonTypeName on each
    // subtype, so the nested "type" is the annotated name, not the class name.
    val paginated = objectMapper.readTree(
      write(
        WebResultUpdateEvent(
          Map("op-page" -> WebPaginationUpdate(PaginationMode(), 7L, List(1, 3))),
          Map.empty
        )
      )
    )
    val update = paginated.get("updates").get("op-page")
    update.get("mode").get("type").asText() shouldBe "PaginationMode"
    update.get("totalNumTuples").asLong() shouldBe 7L
    update.get("dirtyPageIndices").toString shouldBe "[1,3]"

    val delta = objectMapper.readTree(
      write(
        WebResultUpdateEvent(
          Map("op-delta" -> WebDataUpdate(SetDeltaMode(), List.empty)),
          Map.empty
        )
      )
    )
    delta.get("updates").get("op-delta").get("mode").get("type").asText() shouldBe "SetDeltaMode"
  }

  "PythonExpressionEvaluateResponse" should "emit a value object even for an empty proto Option" in {
    // The only Option-typed field reachable from a registered event is scalapb's
    // EvaluatedValue.value. Include.NON_ABSENT would drop an Option.empty, but scalapb's
    // generated `getValue` accessor wins Jackson's bean introspection and yields
    // TypedValue.defaultInstance instead. The Angular EvaluatedValue declares
    // `value: TypedValue` as REQUIRED and dereferences it, so the field must stay
    // present -- and must never be emitted as null.
    val json = objectMapper.readTree(
      write(PythonExpressionEvaluateResponse("1 + 1", Seq(EvaluatedValue(None, Seq.empty))))
    )
    val value = json.get("values").get(0).get("value")
    value.isNull shouldBe false
    value.isObject shouldBe true
    value.get("valueStr").asText() shouldBe ""
    value.get("expandable").asBoolean() shouldBe false
  }

  "WorkflowErrorEvent" should "expose every fatal-error field the Angular client reads" in {
    // The TS WorkflowFatalError reads message/details/operatorId/workerId/type.name and
    // timestamp.{seconds,nanos}; all values below differ so a transposition fails.
    val error = objectMapper
      .readTree(write(WorkflowErrorEvent(Seq(fatalError))))
      .get("fatalErrors")
      .get(0)
    error.get("message").asText() shouldBe "message-1"
    error.get("details").asText() shouldBe "details-2"
    error.get("operatorId").asText() shouldBe "op-3"
    error.get("workerId").asText() shouldBe "worker-4"
    error.get("type").get("name").asText() shouldBe "EXECUTION_FAILURE"
    error.get("timestamp").get("seconds").asLong() shouldBe 1_700_000_000L
  }

  "ConsoleUpdateEvent" should "expose every console-message field the Angular client reads" in {
    val message = objectMapper
      .readTree(write(ConsoleUpdateEvent("op-console", Seq(consoleMessage))))
      .get("messages")
      .get(0)
    message.get("workerId").asText() shouldBe "worker-5"
    message.get("msgType").get("name").asText() shouldBe "PRINT"
    message.get("source").asText() shouldBe "source-6"
    message.get("title").asText() shouldBe "title-7"
    message.get("message").asText() shouldBe "message-8"
  }
}
