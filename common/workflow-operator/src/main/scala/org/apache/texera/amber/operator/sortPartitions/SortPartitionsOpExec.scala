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

package org.apache.texera.amber.operator.sortPartitions

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{AttributeTypeUtils, Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable.ArrayBuffer

class SortPartitionsOpExec(descString: String) extends OperatorExecutor {
  private val desc: SortPartitionsOpDesc =
    objectMapper.readValue(descString, classOf[SortPartitionsOpDesc])
  private var unorderedTuples: ArrayBuffer[Tuple] = _

  override def open(): Unit = {
    unorderedTuples = new ArrayBuffer[Tuple]()
  }

  override def close(): Unit = {
    unorderedTuples.clear()
  }

  private def sortTuples(): Iterator[TupleLike] = unorderedTuples.sortWith(compareTuples).iterator

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    unorderedTuples.append(tuple)
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = sortTuples()

  private def compareTuples(tuple1: Tuple, tuple2: Tuple): Boolean =
    AttributeTypeUtils.compare(
      tuple1.getField[Any](tuple1.getSchema.getIndex(desc.sortAttributeName)),
      tuple2.getField[Any](tuple2.getSchema.getIndex(desc.sortAttributeName)),
      tuple1.getSchema.getAttribute(desc.sortAttributeName).getType
    ) < 0

}
