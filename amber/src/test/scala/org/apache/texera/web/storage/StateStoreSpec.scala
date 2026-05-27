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

package org.apache.texera.web.storage

import org.apache.texera.web.model.websocket.event.TexeraWebSocketEvent
import org.apache.texera.web.model.websocket.response.HeartBeatResponse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class StateStoreSpec extends AnyFlatSpec with Matchers {

  // A dummy event so diff handlers can emit something concrete that the
  // websocket-event observable will carry through to subscribers.
  private case class TaggedEvent(label: String) extends TexeraWebSocketEvent

  "getState" should "return the default state before any updates" in {
    val store = new StateStore[Int](7)
    store.getState shouldBe 7
  }

  "updateState" should "replace state with the function's result" in {
    val store = new StateStore[Int](0)
    store.updateState(_ + 1)
    store.updateState(_ + 10)
    store.getState shouldBe 11
  }

  it should "publish each new value on the state observable" in {
    val store = new StateStore[String]("init")
    val seen = mutable.ListBuffer.empty[String]
    val sub = store.getStateObservable.subscribe(v => seen += v)
    try {
      store.updateState(_ => "a")
      store.updateState(_ => "b")
      // BehaviorSubject replays the current ("init") on subscribe, then
      // delivers the two updates.
      seen.toList shouldBe List("init", "a", "b")
    } finally sub.dispose()
  }

  "getWebsocketEventObservable" should "skip emissions when updateState returns an equal value" in {
    val store = new StateStore[Int](0)
    val fired = new java.util.concurrent.atomic.AtomicInteger(0)
    store.registerDiffHandler { (_, _) =>
      fired.incrementAndGet()
      Iterable.empty
    }
    val sub = store.getWebsocketEventObservable.subscribe(_ => ())
    try {
      store.updateState(_ => 0) // no-op: filter drops, handler should not fire
      store.updateState(_ => 1) // changes: handler fires once
      store.updateState(_ => 1) // no-op again
      fired.get() shouldBe 1
    } finally sub.dispose()
  }

  it should "pass (oldState, newState) into every registered diff handler" in {
    val store = new StateStore[Int](0)
    val pairs = mutable.ListBuffer.empty[(Int, Int)]
    store.registerDiffHandler { (oldS, newS) =>
      pairs += ((oldS, newS))
      Iterable.empty
    }
    val sub = store.getWebsocketEventObservable.subscribe(_ => ())
    try {
      store.updateState(_ => 1)
      store.updateState(_ + 4)
      pairs.toList shouldBe List((0, 1), (1, 5))
    } finally sub.dispose()
  }

  it should "flatten events from multiple diff handlers in registration order" in {
    val store = new StateStore[Int](0)
    store.registerDiffHandler((_, _) => Iterable(TaggedEvent("h1-a"), TaggedEvent("h1-b")))
    store.registerDiffHandler((_, _) => Iterable(TaggedEvent("h2")))
    val emitted = mutable.ListBuffer.empty[String]
    val sub = store.getWebsocketEventObservable.subscribe { evts =>
      evts.foreach { case TaggedEvent(label) => emitted += label; case _ => () }
    }
    try {
      store.updateState(_ + 1)
      emitted.toList shouldBe List("h1-a", "h1-b", "h2")
    } finally sub.dispose()
  }

  "registerDiffHandler" should "return a Disposable that stops the handler from firing" in {
    val store = new StateStore[Int](0)
    val fired = new java.util.concurrent.atomic.AtomicInteger(0)
    val handler = store.registerDiffHandler { (_, _) =>
      fired.incrementAndGet()
      Iterable.empty
    }
    val sub = store.getWebsocketEventObservable.subscribe(_ => ())
    try {
      store.updateState(_ + 1)
      fired.get() shouldBe 1
      handler.dispose()
      store.updateState(_ + 1)
      fired.get() shouldBe 1 // unchanged after dispose
    } finally sub.dispose()
  }

  it should "tolerate double dispose without removing other handlers" in {
    val store = new StateStore[Int](0)
    val countA = new java.util.concurrent.atomic.AtomicInteger(0)
    val countB = new java.util.concurrent.atomic.AtomicInteger(0)
    val handlerA = store.registerDiffHandler { (_, _) =>
      countA.incrementAndGet()
      Iterable.empty
    }
    store.registerDiffHandler { (_, _) =>
      countB.incrementAndGet()
      Iterable.empty
    }
    val sub = store.getWebsocketEventObservable.subscribe(_ => ())
    try {
      handlerA.dispose()
      handlerA.dispose() // no-op
      store.updateState(_ + 1)
      countA.get() shouldBe 0
      countB.get() shouldBe 1
    } finally sub.dispose()
  }

  "getWebsocketEventObservable" should "deliver events to multiple subscribers" in {
    val store = new StateStore[Int](0)
    store.registerDiffHandler((_, _) => Iterable(HeartBeatResponse()))
    val countA = new java.util.concurrent.atomic.AtomicInteger(0)
    val countB = new java.util.concurrent.atomic.AtomicInteger(0)
    val subA = store.getWebsocketEventObservable.subscribe(_ => countA.incrementAndGet())
    val subB = store.getWebsocketEventObservable.subscribe(_ => countB.incrementAndGet())
    try {
      store.updateState(_ + 1)
      store.updateState(_ + 1)
      countA.get() shouldBe 2
      countB.get() shouldBe 2
    } finally {
      subA.dispose()
      subB.dispose()
    }
  }
}
