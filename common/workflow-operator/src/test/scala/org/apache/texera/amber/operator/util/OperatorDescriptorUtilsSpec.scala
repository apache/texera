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

package org.apache.texera.amber.operator.util

import org.scalatest.flatspec.AnyFlatSpec

class OperatorDescriptorUtilsSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // equallyPartitionGoal — shape + invariants
  // ---------------------------------------------------------------------------

  "equallyPartitionGoal" should "return a list whose size equals totalNumWorkers" in {
    assert(OperatorDescriptorUtils.equallyPartitionGoal(10, 3).size == 3)
    assert(OperatorDescriptorUtils.equallyPartitionGoal(0, 5).size == 5)
    assert(OperatorDescriptorUtils.equallyPartitionGoal(100, 1).size == 1)
  }

  it should "produce slots that sum back to the goal" in {
    for {
      goal <- 0 to 20
      workers <- 1 to 5
    } {
      val parts = OperatorDescriptorUtils.equallyPartitionGoal(goal, workers)
      assert(parts.sum == goal, s"sum mismatch for (goal=$goal, workers=$workers): $parts")
    }
  }

  // ---------------------------------------------------------------------------
  // equallyPartitionGoal — distribution semantics (worked cases)
  // ---------------------------------------------------------------------------

  it should "partition evenly when goal is a multiple of totalNumWorkers" in {
    assert(OperatorDescriptorUtils.equallyPartitionGoal(9, 3) == List(3, 3, 3))
    assert(OperatorDescriptorUtils.equallyPartitionGoal(0, 4) == List(0, 0, 0, 0))
    assert(OperatorDescriptorUtils.equallyPartitionGoal(8, 4) == List(2, 2, 2, 2))
  }

  it should "give the remainder to the FIRST `goal % workers` slots" in {
    // 10 = 3*3 + 1 → slot[0] gets the extra
    assert(OperatorDescriptorUtils.equallyPartitionGoal(10, 3) == List(4, 3, 3))
    // 11 = 3*3 + 2 → slots[0,1] each get +1
    assert(OperatorDescriptorUtils.equallyPartitionGoal(11, 3) == List(4, 4, 3))
    // 7 = 3*2 + 1 → slot[0] gets +1
    assert(OperatorDescriptorUtils.equallyPartitionGoal(7, 3) == List(3, 2, 2))
  }

  it should "handle the case where goal < totalNumWorkers" in {
    // 3 = 5*0 + 3 → first 3 slots get 1
    assert(OperatorDescriptorUtils.equallyPartitionGoal(3, 5) == List(1, 1, 1, 0, 0))
    // 1 = 4*0 + 1 → only the first slot gets the single unit
    assert(OperatorDescriptorUtils.equallyPartitionGoal(1, 4) == List(1, 0, 0, 0))
  }

  // ---------------------------------------------------------------------------
  // toImmutableMap — round-trip
  // ---------------------------------------------------------------------------

  "toImmutableMap" should "convert an empty java.util.Map to an empty immutable Map" in {
    val javaMap = new java.util.HashMap[String, Int]()
    val scalaMap = OperatorDescriptorUtils.toImmutableMap(javaMap)
    assert(scalaMap.isEmpty)
  }

  it should "preserve all key/value pairs" in {
    val javaMap = new java.util.LinkedHashMap[String, Integer]()
    javaMap.put("a", Integer.valueOf(1))
    javaMap.put("b", Integer.valueOf(2))
    javaMap.put("c", Integer.valueOf(3))
    val scalaMap = OperatorDescriptorUtils.toImmutableMap(javaMap)
    assert(
      scalaMap == Map(
        "a" -> Integer.valueOf(1),
        "b" -> Integer.valueOf(2),
        "c" -> Integer.valueOf(3)
      )
    )
  }

  it should "return an immutable Map (compile-time enforced)" in {
    val javaMap = new java.util.HashMap[String, Integer]()
    javaMap.put("x", Integer.valueOf(9))
    val scalaMap: scala.collection.immutable.Map[String, Integer] =
      OperatorDescriptorUtils.toImmutableMap(javaMap)
    assert(scalaMap("x") == 9)
  }
}
