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

package edu.uci.ics.amber.operator.split

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.workflow.PortIdentity

import scala.util.Random

class SplitOpExec(
    descString: String
) extends OperatorExecutor {
  val desc: SplitOpDesc = objectMapper.readValue(descString, classOf[SplitOpDesc])
  var random: Random = _

  override def open(): Unit = {
    random = if (desc.random) new Random() else new Random(desc.seed)
  }

  override def close(): Unit = {
    random = null
  }

  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    val isTraining = random.nextInt(100) < desc.k
    // training output port: 0, testing output port: 1
    val port = if (isTraining) PortIdentity(0) else PortIdentity(1)
    Iterator.single((tuple, Some(port)))
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[Tuple] = ???
}
