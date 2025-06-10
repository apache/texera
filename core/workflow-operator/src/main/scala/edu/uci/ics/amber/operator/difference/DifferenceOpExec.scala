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

package edu.uci.ics.amber.operator.difference

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

import scala.collection.mutable

class DifferenceOpExec extends OperatorExecutor {
  private var leftHashSet: mutable.HashSet[Tuple] = _
  private var rightHashSet: mutable.HashSet[Tuple] = _
  private var exhaustedCounter: Int = _

  override def open(): Unit = {
    leftHashSet = new mutable.HashSet()
    rightHashSet = new mutable.HashSet()
    exhaustedCounter = 0
  }

  override def close(): Unit = {
    leftHashSet.clear()
    rightHashSet.clear()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (port == 1) { // right input
      rightHashSet.add(tuple)
    } else { // left input
      leftHashSet.add(tuple)
    }
    Iterator()

  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    exhaustedCounter += 1
    if (2 == exhaustedCounter) {
      leftHashSet.diff(rightHashSet).iterator
    } else {
      Iterator()
    }
  }

}
