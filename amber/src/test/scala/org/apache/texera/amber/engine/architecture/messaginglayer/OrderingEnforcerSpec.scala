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

package org.apache.texera.amber.engine.architecture.messaginglayer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OrderingEnforcerSpec extends AnyFlatSpec with Matchers {

  // ----- initial state -----

  "OrderingEnforcer" should "start with current=0 and an empty stash" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.current shouldBe 0L
    enforcer.ofoMap shouldBe empty
  }

  // ----- setCurrent -----

  "setCurrent" should "advance the current cursor and shift the duplicate threshold" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.setCurrent(10L)
    enforcer.current shouldBe 10L
    enforcer.isDuplicated(9L) shouldBe true
    enforcer.isDuplicated(10L) shouldBe false
  }

  // ----- isDuplicated -----

  "isDuplicated" should "treat sequence numbers below current as duplicates" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.setCurrent(5L)
    enforcer.isDuplicated(0L) shouldBe true
    enforcer.isDuplicated(4L) shouldBe true
  }

  it should "treat sequence numbers >= current that are not stashed as not duplicated" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.setCurrent(5L)
    enforcer.isDuplicated(5L) shouldBe false
    enforcer.isDuplicated(7L) shouldBe false
  }

  it should "report stashed future sequence numbers as duplicated" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.stash(7L, "seven")
    enforcer.isDuplicated(7L) shouldBe true
  }

  // ----- isAhead -----

  "isAhead" should "be true only for sequence numbers strictly greater than current" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.setCurrent(5L)
    enforcer.isAhead(6L) shouldBe true
    enforcer.isAhead(5L) shouldBe false
    enforcer.isAhead(4L) shouldBe false
  }

  // ----- stash -----

  "stash" should "store data under its sequence number for later draining" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.stash(2L, "two")
    enforcer.ofoMap(2L) shouldBe "two"
  }

  it should "overwrite an existing stash entry at the same sequence number" in {
    // Pin: there is no guard against re-stashing the same sequence number.
    // Callers rely on isDuplicated to skip the second stash, but a direct
    // re-stash still overwrites silently.
    val enforcer = new OrderingEnforcer[String]
    enforcer.stash(2L, "first")
    enforcer.stash(2L, "second")
    enforcer.ofoMap(2L) shouldBe "second"
  }

  // ----- enforceFIFO -----

  "enforceFIFO" should "advance current by one and emit just the input when no stash is queued" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.enforceFIFO("zero") shouldBe List("zero")
    enforcer.current shouldBe 1L
  }

  it should "drain a single contiguous stashed entry after the input" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.stash(1L, "one")
    enforcer.enforceFIFO("zero") shouldBe List("zero", "one")
    enforcer.current shouldBe 2L
    enforcer.ofoMap should not contain key(1L)
  }

  it should "drain a contiguous run from the stash and stop at the first gap" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.stash(1L, "one")
    enforcer.stash(2L, "two")
    enforcer.stash(4L, "four") // gap at 3
    val emitted = enforcer.enforceFIFO("zero")
    emitted shouldBe List("zero", "one", "two")
    enforcer.current shouldBe 3L
    enforcer.ofoMap.keys.toList shouldBe List(4L)
  }

  it should "leave the stash untouched when none of the queued entries are contiguous" in {
    val enforcer = new OrderingEnforcer[String]
    enforcer.stash(5L, "five")
    enforcer.stash(7L, "seven")
    val emitted = enforcer.enforceFIFO("zero")
    emitted shouldBe List("zero")
    enforcer.current shouldBe 1L
    enforcer.ofoMap.keys.toSet shouldBe Set(5L, 7L)
  }

  it should "respect a non-zero starting current when draining" in {
    // Setting the cursor manually mimics replay/recovery: the enforcer skips
    // past prior messages and only drains entries with sequence numbers
    // strictly greater than the current value at call time.
    val enforcer = new OrderingEnforcer[String]
    enforcer.setCurrent(10L)
    enforcer.stash(11L, "eleven")
    enforcer.stash(12L, "twelve")
    val emitted = enforcer.enforceFIFO("ten")
    emitted shouldBe List("ten", "eleven", "twelve")
    enforcer.current shouldBe 13L
  }

  it should "support int payloads via the type parameter" in {
    val enforcer = new OrderingEnforcer[Int]
    enforcer.stash(1L, 100)
    enforcer.enforceFIFO(0) shouldBe List(0, 100)
  }
}
