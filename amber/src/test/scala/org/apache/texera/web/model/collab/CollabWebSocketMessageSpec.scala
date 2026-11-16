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

package org.apache.texera.web.model.collab

import com.fasterxml.jackson.databind.exc.InvalidTypeIdException
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.web.model.collab.event._
import org.apache.texera.web.model.collab.request._
import org.apache.texera.web.model.collab.response.HeartBeatResponse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Pins the Jackson polymorphic wire contract for collaboration WebSocket messages.
  *
  * Both [[CollabWebSocketRequest]] and [[CollabWebSocketEvent]] carry `@JsonTypeInfo`
  * on the "type" property with unqualified class names as wire IDs.
  */
class CollabWebSocketMessageSpec extends AnyFlatSpec with Matchers {

  private def readRequest(json: String): CollabWebSocketRequest =
    objectMapper.readValue(json, classOf[CollabWebSocketRequest])

  private def readEvent(json: String): CollabWebSocketEvent =
    objectMapper.readValue(json, classOf[CollabWebSocketEvent])

  private def write(obj: AnyRef): String =
    objectMapper.writeValueAsString(obj)

  "CollabWebSocketRequest" should "deserialize zero-arg request subtypes" in {
    readRequest("""{"type":"AcquireLockRequest"}""") shouldBe AcquireLockRequest()
    readRequest("""{"type":"TryLockRequest"}""") shouldBe TryLockRequest()
    readRequest("""{"type":"HeartBeatRequest"}""") shouldBe HeartBeatRequest()
    readRequest("""{"type":"RestoreVersionRequest"}""") shouldBe RestoreVersionRequest()
  }

  it should "deserialize and serialize CommandRequest with string payload" in {
    val json = """{"type":"CommandRequest","commandMessage":"user-edit-op"}"""
    val req = readRequest(json).asInstanceOf[CommandRequest]
    req.commandMessage shouldBe "user-edit-op"
    val roundTrip = readRequest(write(req)).asInstanceOf[CommandRequest]
    roundTrip.commandMessage shouldBe "user-edit-op"
  }

  it should "deserialize and serialize WIdRequest with integer payload" in {
    val json = """{"type":"WIdRequest","wId":42}"""
    val req = readRequest(json).asInstanceOf[WIdRequest]
    req.wId shouldBe 42
    val roundTrip = readRequest(write(req)).asInstanceOf[WIdRequest]
    roundTrip.wId shouldBe 42
  }

  it should "reject unknown request type IDs" in {
    val ex = intercept[InvalidTypeIdException](readRequest("""{"type":"NonExistentRequest"}"""))
    ex.getMessage should include("NonExistentRequest")
  }

  it should "reject request frames without type property" in {
    val ex = intercept[InvalidTypeIdException](readRequest("""{"commandMessage":"hello"}"""))
    ex.getMessage should include("type")
  }

  "CollabWebSocketEvent" should "deserialize zero-arg event subtypes" in {
    readEvent("""{"type":"LockGrantedEvent"}""") shouldBe LockGrantedEvent()
    readEvent("""{"type":"LockRejectedEvent"}""") shouldBe LockRejectedEvent()
    readEvent("""{"type":"ReleaseLockEvent"}""") shouldBe ReleaseLockEvent()
    readEvent("""{"type":"RestoreVersionEvent"}""") shouldBe RestoreVersionEvent()
  }

  it should "deserialize and serialize CommandEvent with string payload" in {
    val json = """{"type":"CommandEvent","commandMessage":"state-synced"}"""
    val evt = readEvent(json).asInstanceOf[CommandEvent]
    evt.commandMessage shouldBe "state-synced"
    val roundTrip = readEvent(write(evt)).asInstanceOf[CommandEvent]
    roundTrip.commandMessage shouldBe "state-synced"
  }

  it should "deserialize and serialize WorkflowAccessEvent with boolean flag" in {
    val json = """{"type":"WorkflowAccessEvent","workflowReadonly":true}"""
    val evt = readEvent(json).asInstanceOf[WorkflowAccessEvent]
    evt.workflowReadonly shouldBe true
    val roundTrip = readEvent(write(evt)).asInstanceOf[WorkflowAccessEvent]
    roundTrip.workflowReadonly shouldBe true
  }

  it should "deserialize HeartBeatResponse" in {
    val json = """{"type":"HeartBeatResponse"}"""
    val evt = readEvent(json).asInstanceOf[HeartBeatResponse]
    evt shouldBe a[HeartBeatResponse]
  }

  it should "reject unknown event type IDs" in {
    val ex = intercept[InvalidTypeIdException](readEvent("""{"type":"UnknownCollabEvent"}"""))
    ex.getMessage should include("UnknownCollabEvent")
  }

  it should "reject event frames without type property" in {
    val ex = intercept[InvalidTypeIdException](readEvent("""{"workflowReadonly":false}"""))
    ex.getMessage should include("type")
  }
}
