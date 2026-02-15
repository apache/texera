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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.operator.loop.LoopEndOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper

case class Schedule(private val levelSets: Map[Int, Set[Region]]) extends Iterator[Set[Region]] {
  private var currentLevel = levelSets.keys.minOption.getOrElse(0)
  private var loopStartLevel = currentLevel
  private var i = 1

  def getRegions: List[Region] = levelSets.values.flatten.toList

  override def hasNext: Boolean = levelSets.isDefinedAt(currentLevel)

  override def next(): Set[Region] = {
    val regions = levelSets(currentLevel)
    if (
      regions.exists(_.getOperators.exists(_.id.logicalOpId.id.startsWith("LoopStart-operator-")))
    ) loopStartLevel = currentLevel
    currentLevel += 1
    regions
  }

  def loopNext(): Set[Region] = {
    val regions = levelSets(currentLevel)

    if (
      regions.exists(_.getOperators.exists(_.id.logicalOpId.id.startsWith("LoopEnd-operator-")))
    ) {
      if (
        i < objectMapper
          .readValue(
            regions.head.getOperators.head.opExecInitInfo
              .asInstanceOf[OpExecWithClassName]
              .descString,
            classOf[LoopEndOpDesc]
          )
          .iteration
      ) {
        currentLevel = loopStartLevel
        i += 1
      }
    }

    currentLevel += 1
    regions
  }
}
