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
    executionLevels: Vector[Int] = Vector.empty,
    initialLevelIndex: Int = 0
) extends Iterator[Set[Region]] {
  require(
    levelSets.keys.toSet == (0 until levelSets.size).toSet,
    s"Schedule level keys must be contiguous starting at 0, got: ${levelSets.keys.toSeq.sorted}"
  )

  // The actual sequence of levels iterated. Defaults to a single forward pass `0..N-1`;
  // jump-driven extensions append a replay tail to this vector.
  val effectiveExecutionLevels: Vector[Int] =
    if (executionLevels.nonEmpty) executionLevels
    else (0 until levelSets.size).toVector

  private val operatorLevelIndices: Map[OperatorIdentity, Int] =
    levelSets.iterator.flatMap {
      case (level, regions) =>
        regions.iterator.flatMap(region => region.getOperators.map(_.id.logicalOpId -> level))
    }.toMap

  private var currentLevelIndex: Int = initialLevelIndex

  def levelCount: Int = levelSets.size

  def position: Int = currentLevelIndex

  def getRegions: List[Region] = levelSets.values.flatten.toList

  def getLevelIndexOfOperator(opId: OperatorIdentity): Option[Int] = operatorLevelIndices.get(opId)

  override def hasNext: Boolean = effectiveExecutionLevels.isDefinedAt(currentLevelIndex)

  override def next(): Set[Region] = {
    val level = effectiveExecutionLevels(currentLevelIndex)
    currentLevelIndex += 1
    levelSets(level)
  }
}
