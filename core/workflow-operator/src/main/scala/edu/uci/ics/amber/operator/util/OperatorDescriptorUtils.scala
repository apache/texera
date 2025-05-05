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

package edu.uci.ics.amber.operator.util

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object OperatorDescriptorUtils {

  /**
    * Tries to equally partition a integer goal into n total number of workers.
    * In the case that the goal is not a multiple of worker count,
    * this function tries to spread out the remainder evenly to the workers.
    *
    * @param goal            total goal to reach for all workers
    * @param totalNumWorkers total number of workers
    * @return a list which size is equal to totalNumWorkers, each number is the goal assigned for that worker index
    */
  def equallyPartitionGoal(goal: Int, totalNumWorkers: Int): List[Int] = {
    val goalPerWorker =
      mutable.ArrayBuffer.fill(totalNumWorkers)(goal / totalNumWorkers) // integer division
    // divide up the remainder, give 1 to the first n workers
    for (worker <- 0 until goal % totalNumWorkers) {
      goalPerWorker(worker) = goalPerWorker(worker) + 1
    }
    goalPerWorker.toList
  }

  def toImmutableMap[K, V](
      javaMap: java.util.Map[K, V]
  ): scala.collection.immutable.Map[K, V] = {
    javaMap.asScala.toMap
  }

}
