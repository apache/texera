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

package org.apache.texera.common.compiler

import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.common.compiler.model.LogicalPlan

import scala.collection.mutable

/**
  * Which operators of a logical plan lie inside a loop block (LoopStart ... LoopEnd).
  *
  * An operator is inside a block when it lies on a path LoopStart -> ... -> operator -> ... ->
  * LoopEnd whose two ends match. The control operators are not inside their own block, but an
  * inner block's LoopStart and LoopEnd are inside the outer one. Blocks in sequence
  * (S1 -> a -> E1 -> x -> S2 -> b -> E2) do not leak into each other: walking downstream from x,
  * S2 opens a block that E2 closes again, so only a LoopEnd met with no block open closes the
  * operator's own; the upstream walk to the LoopStart is the mirror image.
  *
  * This is the frontend's `getEnclosingLoopStarts` (loop-block.util.ts), which decides where the
  * property panel offers `$K`, so both sides agree on which operators a loop variable reaches.
  * Pure: it reads only the plan's operators and links, never its DAG, so it also ends on a graph
  * with a cycle through a LoopStart or LoopEnd (see `reachesUnmatched`).
  */
object LoopBlockMembership {

  private sealed trait Control
  private case object Start extends Control
  private case object End extends Control

  private def controlOf(op: LogicalOp): Option[Control] =
    op match {
      case _: LoopStartOpDesc => Some(Start)
      case _: LoopEndOpDesc   => Some(End)
      case _                  => None
    }

  /** The operators of `plan` that lie inside some loop block. */
  def operatorsInsideLoopBlocks(plan: LogicalPlan): Set[OperatorIdentity] = {
    val controls: Map[OperatorIdentity, Control] =
      plan.operators.flatMap(op => controlOf(op).map(op.operatorIdentifier -> _)).toMap
    if (!controls.values.exists(_ == Start) || !controls.values.exists(_ == End)) {
      // No block can close: nearly every plan takes this exit.
      Set.empty
    } else {
      val downstream = plan.links.groupMap(_.fromOpId)(_.toOpId)
      val upstream = plan.links.groupMap(_.toOpId)(_.fromOpId)
      plan.operators
        .map(_.operatorIdentifier)
        .filter { id =>
          val control = controls.get(id)
          // Downstream a LoopStart opens a nested block and a LoopEnd closes one; the operator's
          // own block is closed by a LoopEnd met with none open. A LoopStart starts the walk
          // with its own block open. Upstream it is the mirror image.
          reachesUnmatched(id, downstream, controls, Start, End, open(control, Start)) &&
          reachesUnmatched(id, upstream, controls, End, Start, open(control, End))
        }
        .toSet
    }
  }

  private def open(control: Option[Control], opener: Control): Int =
    if (control.contains(opener)) 1 else 0

  /**
    * Whether a breadth-first walk from `origin` over `adjacency` meets a `closes` operator with
    * no block open, counting the blocks opened (`opens`) and closed along each path.
    *
    * A state is a (node, open count) pair, and the open count is capped at the number of
    * `opens` operators plus the blocks open at the start. A path without repeated nodes passes
    * each opener at most once, so on a DAG the count never exceeds the cap and no answer
    * changes; on a cycle through an opener, where every lap would open one more block, the cap
    * keeps the states finite (nodes x (cap + 1)) and the walk ends.
    */
  private def reachesUnmatched(
      origin: OperatorIdentity,
      adjacency: Map[OperatorIdentity, List[OperatorIdentity]],
      controls: Map[OperatorIdentity, Control],
      opens: Control,
      closes: Control,
      initiallyOpen: Int
  ): Boolean = {
    val maxOpen = controls.values.count(_ == opens) + initiallyOpen
    val visited = mutable.Set((origin, initiallyOpen))
    val queue = mutable.Queue((origin, initiallyOpen))
    def visit(state: (OperatorIdentity, Int)): Unit =
      if (visited.add(state)) queue.enqueue(state)
    var found = false
    while (queue.nonEmpty && !found) {
      val (node, openCount) = queue.dequeue()
      val next = adjacency.getOrElse(node, Nil).iterator
      while (next.hasNext && !found) {
        val neighbor = next.next()
        controls.get(neighbor) match {
          case Some(`closes`) if openCount == 0 => found = true
          case Some(`closes`)                   => visit((neighbor, openCount - 1))
          case Some(`opens`)                    =>
            // Only a lap around a cycle exceeds the cap: see above.
            if (openCount < maxOpen) visit((neighbor, openCount + 1))
          case _ => visit((neighbor, openCount))
        }
      }
    }
    found
  }
}
