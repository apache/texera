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

case class Schedule(
    levelSets: Map[Int, Set[Region]],
    initialLevelIndex: Int = 0
) extends Iterator[Set[Region]] {
  require(
    levelSets.keys.toSet == (0 until levelSets.size).toSet,
    s"Schedule level keys must be contiguous starting at 0, got: ${levelSets.keys.toSeq.sorted}"
  )

  private var currentLevel: Int = initialLevelIndex

  def getRegions: List[Region] = levelSets.values.flatten.toList

  override def hasNext: Boolean = levelSets.isDefinedAt(currentLevel)

  override def next(): Set[Region] = {
    val regions = levelSets(currentLevel)
    currentLevel += 1
    regions
  }
}
