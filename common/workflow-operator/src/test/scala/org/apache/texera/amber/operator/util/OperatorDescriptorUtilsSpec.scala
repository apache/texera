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

import java.util

class OperatorDescriptorUtilsSpec extends AnyFlatSpec {

  "OperatorDescriptorUtils.equallyPartitionGoal" should "split an exactly divisible goal evenly" in {
    val partitions = OperatorDescriptorUtils.equallyPartitionGoal(goal = 12, totalNumWorkers = 4)

    assert(partitions == List(3, 3, 3, 3))
    assert(partitions.length == 4)
    assert(partitions.sum == 12)
  }

  it should "assign remainder units to the first workers" in {
    val partitions = OperatorDescriptorUtils.equallyPartitionGoal(goal = 10, totalNumWorkers = 4)

    assert(partitions == List(3, 3, 2, 2))
    assert(partitions.length == 4)
    assert(partitions.sum == 10)
  }

  it should "return all zeroes for a zero goal" in {
    val partitions = OperatorDescriptorUtils.equallyPartitionGoal(goal = 0, totalNumWorkers = 3)

    assert(partitions == List(0, 0, 0))
    assert(partitions.sum == 0)
  }

  it should "return the whole goal for a single worker" in {
    assert(OperatorDescriptorUtils.equallyPartitionGoal(goal = 7, totalNumWorkers = 1) == List(7))
  }

  it should "fail when asked to partition across zero workers" in {
    assertThrows[ArithmeticException] {
      OperatorDescriptorUtils.equallyPartitionGoal(goal = 7, totalNumWorkers = 0)
    }
  }

  "OperatorDescriptorUtils.toImmutableMap" should "convert an empty java map to an empty Scala map" in {
    val javaMap = new util.LinkedHashMap[String, Int]()

    assert(OperatorDescriptorUtils.toImmutableMap(javaMap).isEmpty)
  }

  it should "copy all entries from a populated java map" in {
    val javaMap = new util.LinkedHashMap[String, Int]()
    javaMap.put("first", 1)
    javaMap.put("second", 2)

    assert(OperatorDescriptorUtils.toImmutableMap(javaMap) == Map("first" -> 1, "second" -> 2))
  }

  it should "not reflect later mutations to the source java map" in {
    val javaMap = new util.LinkedHashMap[String, Int]()
    javaMap.put("original", 1)

    val immutableMap = OperatorDescriptorUtils.toImmutableMap(javaMap)
    javaMap.put("later", 2)

    assert(immutableMap == Map("original" -> 1))
  }
}
