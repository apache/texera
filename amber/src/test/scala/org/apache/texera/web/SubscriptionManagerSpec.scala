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

import io.reactivex.rxjava3.disposables.Disposable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger

class SubscriptionManagerSpec extends AnyFlatSpec with Matchers {

  // Minimal concrete subject — SubscriptionManager is a trait, and the
  // production mixins (SessionState, WorkflowService, ...) drag in heavy
  // dependencies that aren't part of what this spec is exercising.
  private class TestManager extends SubscriptionManager

  // A Disposable that counts dispose() invocations so the spec can tell
  // "disposed exactly once" from "disposed twice" without resorting to
  // Mockito.
  private class CountingDisposable extends Disposable {
    private val disposed = new AtomicInteger(0)
    override def dispose(): Unit = disposed.incrementAndGet()
    override def isDisposed: Boolean = disposed.get() > 0
    def disposeCount: Int = disposed.get()
  }

  "unsubscribeAll" should "dispose every added subscription in insertion order" in {
    val mgr = new TestManager
    val order = scala.collection.mutable.ListBuffer.empty[String]
    val a = Disposable.fromAction(() => order += "a")
    val b = Disposable.fromAction(() => order += "b")
    val c = Disposable.fromAction(() => order += "c")
    mgr.addSubscription(a)
    mgr.addSubscription(b)
    mgr.addSubscription(c)
    mgr.unsubscribeAll()
    order.toList shouldBe List("a", "b", "c")
  }

  it should "clear the internal buffer so a second call is a no-op" in {
    val mgr = new TestManager
    val d = new CountingDisposable
    mgr.addSubscription(d)
    mgr.unsubscribeAll()
    mgr.unsubscribeAll()
    d.disposeCount shouldBe 1
  }

  it should "do nothing when no subscriptions have been added" in {
    val mgr = new TestManager
    noException should be thrownBy mgr.unsubscribeAll()
  }

  it should "let new subscriptions accumulate after a previous unsubscribeAll" in {
    val mgr = new TestManager
    val first = new CountingDisposable
    val second = new CountingDisposable
    mgr.addSubscription(first)
    mgr.unsubscribeAll()
    mgr.addSubscription(second)
    mgr.unsubscribeAll()
    first.disposeCount shouldBe 1
    second.disposeCount shouldBe 1
  }
}
