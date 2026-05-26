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

package org.apache.texera.web

import org.apache.texera.web.model.websocket.request.{
  HeartBeatRequest,
  TexeraWebSocketRequest,
  WorkflowKillRequest
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

class WebsocketInputSpec extends AnyFlatSpec with Matchers {

  // Each test gets a fresh input + error sink. Throwables routed through
  // `errorHandler` land in `errors` for assertion.
  private def newInputWithErrorSink(): (WebsocketInput, mutable.ListBuffer[Throwable]) = {
    val errors = mutable.ListBuffer.empty[Throwable]
    (new WebsocketInput(errors += _), errors)
  }

  "subscribe" should "deliver requests whose runtime class matches T" in {
    val (input, errors) = newInputWithErrorSink()
    val received = mutable.ListBuffer.empty[(HeartBeatRequest, Option[Integer])]
    val sub = input.subscribe[HeartBeatRequest]((req, uid) => received += ((req, uid)))
    try {
      val req = HeartBeatRequest()
      input.onNext(req, Some(7))
      received.toList shouldBe List((req, Some(7)))
      errors shouldBe empty
    } finally sub.dispose()
  }

  it should "silently drop requests that do not match T" in {
    val (input, errors) = newInputWithErrorSink()
    val received = mutable.ListBuffer.empty[TexeraWebSocketRequest]
    val sub = input.subscribe[HeartBeatRequest]((req, _) => received += req)
    try {
      input.onNext(WorkflowKillRequest(), None)
      input.onNext(WorkflowKillRequest(), Some(1))
      received shouldBe empty
      errors shouldBe empty
    } finally sub.dispose()
  }

  it should "pass through uidOpt verbatim, including None" in {
    val (input, _) = newInputWithErrorSink()
    val seenUids = mutable.ListBuffer.empty[Option[Integer]]
    val sub = input.subscribe[HeartBeatRequest]((_, uid) => seenUids += uid)
    try {
      input.onNext(HeartBeatRequest(), None)
      input.onNext(HeartBeatRequest(), Some(42))
      // Integer is a Java boxed type; compare via .map(_.intValue) to
      // avoid hinging on Integer identity.
      seenUids.map(_.map(_.intValue)).toList shouldBe List(None, Some(42))
    } finally sub.dispose()
  }

  "subscribe" should "route exceptions thrown inside the callback to errorHandler" in {
    val (input, errors) = newInputWithErrorSink()
    val boom = new RuntimeException("boom")
    val sub = input.subscribe[HeartBeatRequest]((_, _) => throw boom)
    try {
      input.onNext(HeartBeatRequest(), None)
      errors.toList shouldBe List(boom)
    } finally sub.dispose()
  }

  it should "keep delivering events to other subscribers after one callback throws" in {
    val (input, errors) = newInputWithErrorSink()
    val survivorCount = new AtomicReference[Int](0)
    val throwingSub =
      input.subscribe[HeartBeatRequest]((_, _) => throw new IllegalStateException("nope"))
    val survivorSub = input.subscribe[HeartBeatRequest]((_, _) => survivorCount.updateAndGet(_ + 1))
    try {
      input.onNext(HeartBeatRequest(), None)
      input.onNext(HeartBeatRequest(), None)
      survivorCount.get() shouldBe 2
      errors should have size 2
    } finally {
      throwingSub.dispose()
      survivorSub.dispose()
    }
  }

  it should "stop delivering events after the returned Disposable is disposed" in {
    val (input, _) = newInputWithErrorSink()
    val count = new AtomicReference[Int](0)
    val sub = input.subscribe[HeartBeatRequest]((_, _) => count.updateAndGet(_ + 1))
    input.onNext(HeartBeatRequest(), None)
    sub.dispose()
    input.onNext(HeartBeatRequest(), None)
    input.onNext(HeartBeatRequest(), None)
    count.get() shouldBe 1
  }
}
