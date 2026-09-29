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

package org.apache.texera.amber.operator.reservoirsampling

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.operator.util.OperatorDescriptorUtils.equallyPartitionGoal
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.util.Random

class ReservoirSamplingOpExec(descString: String, idx: Int, workerCount: Int)
    extends OperatorExecutor {
  private val desc: ReservoirSamplingOpDesc =
    objectMapper.readValue(descString, classOf[ReservoirSamplingOpDesc])
  // Read at first use, not at construction or in open(): inside a loop block `k` may refer to a
  // loop variable, which the loop state writes into the setting after both.
  private lazy val count: Int = equallyPartitionGoal(desc.k, workerCount)(idx)
  private var n: Int = _
  private var reservoir: Array[Tuple] = _
  private val rand: Random = new Random(workerCount)

  override def open(): Unit = {
    n = 0
    reservoir = null
  }

  /** The reservoir, allocated at first use. */
  private def slots: Array[Tuple] = {
    if (reservoir == null) reservoir = Array.ofDim(count)
    reservoir
  }

  override def close(): Unit = {
    reservoir = null
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    val sample = slots
    if (n < count) {
      sample(n) = tuple
    } else {
      val i = rand.nextInt(n)
      if (i < count) {
        sample(i) = tuple
      }
    }
    n += 1
    Iterator()
  }

  // Only the first n slots are filled when the input is smaller than the reservoir;
  // take(n) keeps the trailing unfilled (null) slots from being emitted.
  override def onFinish(port: Int): Iterator[TupleLike] = slots.iterator.take(n)

}
