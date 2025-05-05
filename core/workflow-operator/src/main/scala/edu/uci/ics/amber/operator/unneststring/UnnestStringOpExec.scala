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

package edu.uci.ics.amber.operator.unneststring

import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.operator.flatmap.FlatMapOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class UnnestStringOpExec(descString: String) extends FlatMapOpExec {
  private val desc: UnnestStringOpDesc =
    objectMapper.readValue(descString, classOf[UnnestStringOpDesc])
  setFlatMapFunc(splitByDelimiter)

  private def splitByDelimiter(tuple: Tuple): Iterator[TupleLike] = {
    desc.delimiter.r
      .split(tuple.getField(desc.attribute).toString)
      .filter(_.nonEmpty)
      .iterator
      .map(split => TupleLike(tuple.getFields ++ Seq(split)))
  }
}
