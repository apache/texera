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

private object ScheduleStep {
  sealed trait Step
  case class Batch(regions: Set[Region]) extends Step
  case class Barrier(start: OperatorIdentity) extends Step
}

case class Schedule(
    levelSets: Map[Int, Set[Region]],
    initialLevelIndex: Int = 0,
    loopMembers: Map[OperatorIdentity, Set[RegionIdentity]] = Map.empty
) extends Iterator[Set[Region]] {
  require(
    levelSets.keys.toSet == (0 until levelSets.size).toSet,
    s"Schedule level keys must be contiguous starting at 0, got: ${levelSets.keys.toSeq.sorted}"
  )

  import ScheduleStep._

  private lazy val levels = List.tabulate(levelSets.size)(i => i -> levelSets(i))
  private lazy val levelOf = levels.flatMap {
    case (level, batch) => batch.map(_.id -> level)
  }.toMap
  private lazy val closingAt =
    loopMembers.keys.toList.groupBy(start => loopMembers(start).map(levelOf).max)
  private def steps(fromLevel: Int): List[Step] =
    levels.drop(fromLevel).flatMap {
      case (level, batch) =>
        // If nested loops finish in one batch, an inner repeat must drain before the outer barrier.
        Batch(batch) :: closingAt
          .getOrElse(level, Nil)
          .sortBy(start => (loopMembers(start).size, start.id))
          .map(Barrier)
    }

  private var currentLevel = initialLevelIndex
  private lazy val allSteps = steps(0)
  private var remaining = if (loopMembers.isEmpty) Nil else steps(initialLevelIndex)
  private var requested = Set.empty[OperatorIdentity]
  private lazy val replay = loopMembers.map {
    case (start, members) =>
      start -> allSteps.flatMap {
        case Batch(batch) =>
          val selected = batch.filter(r => members(r.id))
          Option.when(selected.nonEmpty)(Batch(selected))
        case b @ Barrier(id) => Option.when(loopMembers(id).subsetOf(members))(b)
      }
  }

  // Loop End reports its decision while its region is running. Dispatch consumes the
  // request at the barrier after the last body batch (which may contain a side sink).
  def jumpTo(target: OperatorIdentity): Unit = {
    if (loopMembers.contains(target)) requested += target
    else
      levels
        .collectFirst {
          case (level, batch) if batch.exists(_.getOperators.exists(_.id.logicalOpId == target)) =>
            level
        }
        .foreach { level =>
          currentLevel = level
          requested = Set.empty
          if (loopMembers.nonEmpty) remaining = steps(level)
        }
  }

  def getRegions: List[Region] = levelSets.values.flatten.toList

  override def hasNext: Boolean =
    if (loopMembers.isEmpty) levelSets.contains(currentLevel)
    else
      remaining.exists {
        case Batch(_)       => true
        case Barrier(start) => requested(start)
      }

  override def next(): Set[Region] = {
    if (loopMembers.nonEmpty) return nextLoopBatch()
    val batch = levelSets(currentLevel)
    currentLevel += 1
    batch
  }

  @scala.annotation.tailrec
  private def nextLoopBatch(): Set[Region] =
    remaining match {
      case Batch(batch) :: tail =>
        remaining = tail
        batch
      case Barrier(start) :: tail =>
        remaining = if (requested(start)) replay(start) ::: tail else tail
        requested -= start
        nextLoopBatch()
      case Nil => throw new NoSuchElementException("no remaining region batches")
    }
}
