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

package org.apache.texera.amber.operator.set

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}

import scala.collection.mutable

abstract class BinarySetOpExec extends OperatorExecutor {
  private val leftSet = new mutable.HashSet[Tuple]()
  private val rightSet = new mutable.HashSet[Tuple]()
  private var exhaustedCounter = 0

  override def open(): Unit = {
    close()
    exhaustedCounter = 0
  }

  override def close(): Unit = {
    leftSet.clear()
    rightSet.clear()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    (if (port == 0) leftSet else rightSet) += tuple
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    exhaustedCounter += 1
    if (exhaustedCounter == 2) combine(leftSet, rightSet) else Iterator()
  }

  protected def combine(
      leftSet: mutable.HashSet[Tuple],
      rightSet: mutable.HashSet[Tuple]
  ): Iterator[TupleLike]
}
