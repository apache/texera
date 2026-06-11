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

package org.apache.texera.amber.engine.architecture.deploysemantics.deploystrategy

import org.apache.pekko.actor.Address
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeployStrategiesSpec extends AnyFlatSpec with Matchers {

  // Use the "pekko" protocol to match Amber's real node addresses
  // (e.g. AmberConfig.masterNodeAddr); "akka" diverges from production and
  // can mislead anyone who debugs a failure by comparing addresses.
  private val nodeA = Address("pekko", "sys", "host-a", 2552)
  private val nodeB = Address("pekko", "sys", "host-b", 2552)
  private val nodeC = Address("pekko", "sys", "host-c", 2552)

  // ----- OneOnEach -----

  "OneOnEach" should "hand out each address exactly once in array order" in {
    val strategy = OneOnEach()
    strategy.initialize(Array(nodeA, nodeB, nodeC))
    strategy.next() shouldBe nodeA
    strategy.next() shouldBe nodeB
    strategy.next() shouldBe nodeC
  }

  it should "raise NoSuchElementException once the array is exhausted" in {
    val strategy = OneOnEach()
    strategy.initialize(Array(nodeA))
    strategy.next() shouldBe nodeA
    assertThrows[NoSuchElementException](strategy.next())
  }

  it should "raise NoSuchElementException immediately when initialized with an empty array" in {
    val strategy = OneOnEach()
    strategy.initialize(Array.empty[Address])
    assertThrows[NoSuchElementException](strategy.next())
  }

  it should "reset its iteration cursor on re-initialization" in {
    val strategy = OneOnEach()
    strategy.initialize(Array(nodeA, nodeB))
    strategy.next() shouldBe nodeA
    strategy.next() shouldBe nodeB
    strategy.initialize(Array(nodeC))
    strategy.next() shouldBe nodeC
    assertThrows[NoSuchElementException](strategy.next())
  }

  "OneOnEach.apply" should "produce a fresh, independent instance" in {
    val s1 = OneOnEach()
    val s2 = OneOnEach()
    s1 should not be theSameInstanceAs(s2)
  }

  // ----- RoundRobinDeployment -----

  "RoundRobinDeployment" should "rotate addresses in a repeating cycle" in {
    val strategy = RoundRobinDeployment()
    strategy.initialize(Array(nodeA, nodeB, nodeC))
    strategy.next() shouldBe nodeA
    strategy.next() shouldBe nodeB
    strategy.next() shouldBe nodeC
    strategy.next() shouldBe nodeA
    strategy.next() shouldBe nodeB
  }

  it should "always return the only address when the array has length 1" in {
    val strategy = RoundRobinDeployment()
    strategy.initialize(Array(nodeA))
    for (_ <- 1 to 5) strategy.next() shouldBe nodeA
  }

  it should "raise NoSuchElementException on next() with an empty array" in {
    val strategy = RoundRobinDeployment()
    strategy.initialize(Array.empty[Address])
    assertThrows[NoSuchElementException](strategy.next())
  }

  "RoundRobinDeployment.apply" should "produce a fresh, independent instance" in {
    val s1 = RoundRobinDeployment()
    val s2 = RoundRobinDeployment()
    s1 should not be theSameInstanceAs(s2)
  }

  // ----- RandomDeployment -----

  "RandomDeployment" should "always return one of the available addresses" in {
    val strategy = RandomDeployment()
    val pool = Array(nodeA, nodeB, nodeC)
    strategy.initialize(pool)
    val poolSet = pool.toSet
    for (_ <- 1 to 50) {
      poolSet should contain(strategy.next())
    }
  }

  it should "always return the only address when the array has length 1" in {
    val strategy = RandomDeployment()
    strategy.initialize(Array(nodeA))
    for (_ <- 1 to 5) strategy.next() shouldBe nodeA
  }

  it should "raise NoSuchElementException on next() with an empty array" in {
    val strategy = RandomDeployment()
    strategy.initialize(Array.empty[Address])
    assertThrows[NoSuchElementException](strategy.next())
  }

  "RandomDeployment.apply" should "produce a fresh, independent instance" in {
    val s1 = RandomDeployment()
    val s2 = RandomDeployment()
    s1 should not be theSameInstanceAs(s2)
  }
}
