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

package edu.uci.ics.amber.operator.reservoirsampling

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.operator.util.OperatorDescriptorUtils.equallyPartitionGoal
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.util.Random

class ReservoirSamplingOpExec(descString: String, idx: Int, workerCount: Int)
    extends OperatorExecutor {
  private val desc: ReservoirSamplingOpDesc =
    objectMapper.readValue(descString, classOf[ReservoirSamplingOpDesc])
  private val count: Int = equallyPartitionGoal(desc.k, workerCount)(idx)
  private var n: Int = _
  private var reservoir: Array[Tuple] = _
  private val rand: Random = new Random(workerCount)

  override def open(): Unit = {
    n = 0
    reservoir = Array.ofDim(count)
  }

  override def close(): Unit = {
    reservoir = null
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    if (n < count) {
      reservoir(n) = tuple
    } else {
      val i = rand.nextInt(n)
      if (i < count) {
        reservoir(i) = tuple
      }
    }
    n += 1
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = reservoir.iterator

}
