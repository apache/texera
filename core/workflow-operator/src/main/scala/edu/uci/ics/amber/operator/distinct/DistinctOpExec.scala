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

package edu.uci.ics.amber.operator.distinct

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

import scala.collection.mutable

/**
  * An executor for the distinct operation that filters out duplicate tuples.
  * It uses a `LinkedHashSet` to preserve the input order while removing duplicates.
  */
class DistinctOpExec extends OperatorExecutor {
  private var seenTuples: mutable.LinkedHashSet[Tuple] = _

  override def open(): Unit = {
    seenTuples = mutable.LinkedHashSet()
  }

  override def close(): Unit = {
    seenTuples.clear()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    seenTuples.add(tuple)
    Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    seenTuples.iterator
  }

}
