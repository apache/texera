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

package edu.uci.ics.amber.operator.hashJoin

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class HashJoinBuildOpExec[K](descString: String) extends OperatorExecutor {
  private val desc: HashJoinOpDesc[K] =
    objectMapper.readValue(descString, classOf[HashJoinOpDesc[K]])
  var buildTableHashMap: mutable.HashMap[K, ListBuffer[Tuple]] = _

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, mutable.ListBuffer[Tuple]]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    val key = tuple.getField(desc.buildAttributeName).asInstanceOf[K]
    buildTableHashMap.getOrElseUpdate(key, new ListBuffer[Tuple]()) += tuple
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    buildTableHashMap.iterator.flatMap {
      case (k, v) => v.map(t => TupleLike(List(k) ++ t.getFields))
    }
  }
}
