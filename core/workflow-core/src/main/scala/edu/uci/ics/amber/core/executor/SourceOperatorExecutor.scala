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

package edu.uci.ics.amber.core.executor

import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.core.workflow.PortIdentity

trait SourceOperatorExecutor extends OperatorExecutor {
  override def open(): Unit = {}

  override def close(): Unit = {}
  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = Iterator()

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty

  def produceTuple(): Iterator[TupleLike]

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    // We assume there is only one input port for source operators. The current assumption
    // makes produceTuple to be invoked on each input port finish.
    // We should move this to onFinishAllPorts later.
    produceTuple().map(t => (t, Option.empty))
  }
}
