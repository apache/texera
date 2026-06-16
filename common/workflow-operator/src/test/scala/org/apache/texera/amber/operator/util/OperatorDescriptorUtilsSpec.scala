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
  // equallyPartitionGoal — exact / inexact division
  // ---------------------------------------------------------------------------

  "OperatorDescriptorUtils.equallyPartitionGoal" should
    "split a goal evenly when it divides cleanly by the worker count" in {
    val parts = OperatorDescriptorUtils.equallyPartitionGoal(100, 4)
    assert(parts == List(25, 25, 25, 25))
  }

  it should
    "spread the remainder one-each to the first (goal % workers) workers when uneven" in {
    // goal = 10, workers = 3 -> floor = 3, remainder = 1 -> [4, 3, 3]
    val parts = OperatorDescriptorUtils.equallyPartitionGoal(10, 3)
    assert(parts == List(4, 3, 3))
  }

  it should "give the first two workers an extra 1 when remainder = 2" in {
    // goal = 11, workers = 3 -> floor = 3, remainder = 2 -> [4, 4, 3]
    val parts = OperatorDescriptorUtils.equallyPartitionGoal(11, 3)
    assert(parts == List(4, 4, 3))
  }

  it should "always return a list whose length equals totalNumWorkers" in {
    assert(OperatorDescriptorUtils.equallyPartitionGoal(10, 1).length == 1)
    assert(OperatorDescriptorUtils.equallyPartitionGoal(10, 7).length == 7)
    assert(OperatorDescriptorUtils.equallyPartitionGoal(0, 5).length == 5)
  }

  it should "always sum to the original goal" in {
    val cases = List((100, 4), (10, 3), (11, 3), (0, 5), (1, 5), (7, 1), (50, 50))
    cases.foreach {
      case (goal, workers) =>
        val parts = OperatorDescriptorUtils.equallyPartitionGoal(goal, workers)
        assert(parts.sum == goal, s"sum mismatch for goal=$goal workers=$workers got $parts")
    }
  }

  it should "return all zeros when goal = 0" in {
    assert(OperatorDescriptorUtils.equallyPartitionGoal(0, 5) == List(0, 0, 0, 0, 0))
  }

  it should "concentrate the entire goal in the single worker when totalNumWorkers = 1" in {
    assert(OperatorDescriptorUtils.equallyPartitionGoal(42, 1) == List(42))
  }

  // ---------------------------------------------------------------------------
  // toImmutableMap — round-trip with isolation
  // ---------------------------------------------------------------------------

  "OperatorDescriptorUtils.toImmutableMap" should "return Map.empty for an empty java.util.Map" in {
    val empty = new java.util.HashMap[String, Int]()
    val result: Map[String, Int] = OperatorDescriptorUtils.toImmutableMap(empty)
    assert(result.isEmpty)
  }

  it should "preserve every entry of a populated java.util.Map" in {
    val src = new java.util.HashMap[String, Int]()
    src.put("a", 1)
    src.put("b", 2)
    src.put("c", 3)
    val result = OperatorDescriptorUtils.toImmutableMap(src)
    assert(result == Map("a" -> 1, "b" -> 2, "c" -> 3))
  }

  it should
    "isolate the returned map from subsequent mutation of the source java.util.Map" in {
    // `asScala.toMap` materializes a Scala immutable Map at call time;
    // mutating the source after the conversion must not leak into the
    // returned map (a regression to a lazy view would break this).
    val src = new java.util.HashMap[String, Int]()
    src.put("k", 1)
    val converted = OperatorDescriptorUtils.toImmutableMap(src)
    src.put("k", 999)
    src.put("new", 7)
    assert(converted == Map("k" -> 1))
  }

  it should "be typed as scala.collection.immutable.Map (compile-time enforced)" in {
    val src = new java.util.HashMap[String, Int]()
    src.put("k", 1)
    val result: scala.collection.immutable.Map[String, Int] =
      OperatorDescriptorUtils.toImmutableMap(src)
    assert(result.contains("k"))
  }
}
