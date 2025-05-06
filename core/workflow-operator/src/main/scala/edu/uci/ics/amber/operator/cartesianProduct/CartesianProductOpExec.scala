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

package edu.uci.ics.amber.operator.cartesianProduct

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.operator.hashJoin.JoinUtils
import scala.collection.mutable.ArrayBuffer

/**
  * Executes a Cartesian Product operation between tuples from two input streams.
  */
class CartesianProductOpExec extends OperatorExecutor {
  private var leftTuples: ArrayBuffer[Tuple] = _

  override def open(): Unit = {
    leftTuples = ArrayBuffer[Tuple]()
  }

  override def close(): Unit = {
    leftTuples.clear()
  }

  /**
    * Processes incoming tuples from either the left or right input stream.
    * Tuples from the left stream are collected until the stream is exhausted.
    * Then, for each tuple from the right stream, it generates a Cartesian product
    * with every tuple collected from the left stream.
    *
    * @param tuple Either a Tuple from one of the streams or an InputExhausted signal.
    * @param port  The port number indicating which stream the tuple is from (0 for left, 1 for right).
    * @return An Iterator of TupleLike objects representing the Cartesian product.
    */
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (port == 0) {
      leftTuples += tuple
      Iterator.empty
    } else {
      leftTuples.map(leftTuple => JoinUtils.joinTuples(leftTuple, tuple)).iterator
    }
  }
}
