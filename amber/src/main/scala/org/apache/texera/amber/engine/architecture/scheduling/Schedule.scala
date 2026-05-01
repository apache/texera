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

import org.apache.texera.amber.core.virtualidentity.OperatorIdentity

case class Schedule(
    private val levelSets: Map[Int, Set[Region]],
    executionLevels: Vector[Int] = Vector.empty
) extends Iterator[Set[Region]] {
  require(
    levelSets.keys.toSet == (0 until levelSets.size).toSet,
    s"Schedule level keys must be contiguous starting at 0, got: ${levelSets.keys.toSeq.sorted}"
  )

  private val baseLevels = levelSets.keys.toVector.sorted
  private val normalizedExecutionLevels =
    if (executionLevels.nonEmpty || baseLevels.isEmpty) executionLevels else baseLevels
  private val operatorLevelIndices = levelSets.iterator.flatMap {
    case (level, regions) =>
      val levelIndex = baseLevels.indexOf(level)
      regions.iterator.flatMap(region => region.getOperators.map(_.id.logicalOpId -> levelIndex))
  }.toMap
  private var currentLevelIndex = 0

  def getRegions: List[Region] = levelSets.values.flatten.toList

  def getLevelIndexOfOperator(opId: OperatorIdentity): Option[Int] = operatorLevelIndices.get(opId)

  def rewriteExecutionFrom(levelIndex: Int): Schedule = {
    val rewrittenSchedule = copy(
      executionLevels =
        normalizedExecutionLevels.take(currentLevelIndex) ++ baseLevels.drop(levelIndex)
    )
    rewrittenSchedule.currentLevelIndex = currentLevelIndex
    rewrittenSchedule
  }

  override def hasNext: Boolean = currentLevelIndex < normalizedExecutionLevels.length

  override def next(): Set[Region] = {
    val regions = normalizedExecutionLevels
      .lift(currentLevelIndex)
      .flatMap(levelSets.get)
      .getOrElse(Set.empty)
    currentLevelIndex += 1
    regions
  }
}
