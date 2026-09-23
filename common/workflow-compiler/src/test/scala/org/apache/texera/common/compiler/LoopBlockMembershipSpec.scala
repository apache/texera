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
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlan}
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Block membership as the compiler computes it. The cases mirror the frontend's
  * `getEnclosingLoopStarts` spec (loop-block.util.ts), which this is a port of: an operator is
  * inside a block when it lies on a path LoopStart -> ... -> operator -> ... -> LoopEnd whose
  * two ends match.
  */
class LoopBlockMembershipSpec extends AnyFlatSpec {

  private def start(id: String): LogicalOp = named(new LoopStartOpDesc, id)
  private def end(id: String): LogicalOp = named(new LoopEndOpDesc, id)
  private def op(id: String): LogicalOp = named(new LimitOpDesc, id)

  private def named(op: LogicalOp, id: String): LogicalOp = {
    op.setOperatorId(id)
    op
  }

  /** `edges` as "a->b" pairs; every operator is looked up by its id in `operators`. */
  private def inside(operators: List[LogicalOp], edges: (String, String)*): Set[String] = {
    val links = edges.toList.map {
      case (from, to) =>
        LogicalLink(OperatorIdentity(from), PortIdentity(), OperatorIdentity(to), PortIdentity())
    }
    // A LogicalPlan builds its DAG lazily, and membership never asks for it: a cycle is fine.
    LoopBlockMembership.operatorsInsideLoopBlocks(LogicalPlan(operators, links)).map(_.id)
  }

  "LoopBlockMembership" should "find nothing in a plan without loop operators" in {
    assert(inside(List(op("a"), op("b")), "a" -> "b").isEmpty)
  }

  it should "put the body of a straight block inside it, and neither control operator" in {
    // src -> S -> a -> b -> E -> sink
    val ops = List(op("src"), start("S"), op("a"), op("b"), end("E"), op("sink"))
    val result =
      inside(ops, "src" -> "S", "S" -> "a", "a" -> "b", "b" -> "E", "E" -> "sink")
    assert(result == Set("a", "b"))
  }

  it should "leave out an operator upstream of the LoopStart and one downstream of the LoopEnd" in {
    val ops = List(op("up"), start("S"), op("a"), end("E"), op("down"))
    val result = inside(ops, "up" -> "S", "S" -> "a", "a" -> "E", "E" -> "down")
    assert(!result.contains("up"))
    assert(!result.contains("down"))
    assert(result == Set("a"))
  }

  it should "put both branches of a block that branches and converges inside it" in {
    // S -> a -> E, S -> b -> c -> E
    val ops = List(start("S"), op("a"), op("b"), op("c"), end("E"))
    val result = inside(ops, "S" -> "a", "a" -> "E", "S" -> "b", "b" -> "c", "c" -> "E")
    assert(result == Set("a", "b", "c"))
  }

  it should "leave out a dangling branch that never reaches the LoopEnd" in {
    // S -> a -> E, and S -> d -> e: d and e hang off the LoopStart but the block never closes
    // on their path, so no state bound for the block is guaranteed to mean anything to them.
    val ops = List(start("S"), op("a"), end("E"), op("d"), op("e"))
    val result = inside(ops, "S" -> "a", "a" -> "E", "S" -> "d", "d" -> "e")
    assert(result == Set("a"))
  }

  it should "leave out a branch that joins the body from outside the block" in {
    // src2 -> a, where a is in the body of S ... E: src2 reaches E but no LoopStart reaches src2.
    val ops = List(start("S"), op("a"), end("E"), op("src2"))
    val result = inside(ops, "S" -> "a", "a" -> "E", "src2" -> "a")
    assert(result == Set("a"))
  }

  it should "put a nested block's LoopStart and LoopEnd inside the outer block, and not the outer ones" in {
    // S0 -> x -> S1 -> a -> E1 -> y -> E0
    val ops =
      List(start("S0"), op("x"), start("S1"), op("a"), end("E1"), op("y"), end("E0"))
    val result =
      inside(ops, "S0" -> "x", "x" -> "S1", "S1" -> "a", "a" -> "E1", "E1" -> "y", "y" -> "E0")
    assert(result == Set("x", "S1", "a", "E1", "y"))
  }

  it should "not leak one block into the next when blocks run in sequence" in {
    // S1 -> a -> E1 -> x -> S2 -> b -> E2: walking downstream from x, S2 opens a block that E2
    // closes again, and upstream E1 opens one that S1 closes; x is in neither block.
    val ops =
      List(start("S1"), op("a"), end("E1"), op("x"), start("S2"), op("b"), end("E2"))
    val result =
      inside(ops, "S1" -> "a", "a" -> "E1", "E1" -> "x", "x" -> "S2", "S2" -> "b", "b" -> "E2")
    assert(result == Set("a", "b"))
  }

  it should "not count blocks in sequence whose control operators are directly linked" in {
    // S1 -> a -> E1 -> S2 -> b -> E2: E1 and S2 are control operators of different blocks.
    val ops = List(start("S1"), op("a"), end("E1"), start("S2"), op("b"), end("E2"))
    val result =
      inside(ops, "S1" -> "a", "a" -> "E1", "E1" -> "S2", "S2" -> "b", "b" -> "E2")
    assert(result == Set("a", "b"))
  }

  it should "leave everything out when the LoopStart has no LoopEnd, or the LoopEnd no LoopStart" in {
    assert(inside(List(start("S"), op("a"), op("b")), "S" -> "a", "a" -> "b").isEmpty)
    assert(inside(List(op("a"), op("b"), end("E")), "a" -> "b", "b" -> "E").isEmpty)
  }

  // A LogicalPlan is a DAG by the time it is compiled, but membership is a pure walk over any
  // operators and links. A cycle through a control operator opens one more block on every lap,
  // so without the cap on the open count the walk would never revisit a state and never end.

  it should "end on a cycle in the body and keep both of its operators inside" in {
    // S -> a <-> b -> E
    val ops = List(start("S"), op("a"), op("b"), end("E"))
    assert(inside(ops, "S" -> "a", "a" -> "b", "b" -> "a", "b" -> "E") == Set("a", "b"))
  }

  it should "end on a back edge from the body into its LoopStart" in {
    // S -> a -> E, a -> S
    val ops = List(start("S"), op("a"), end("E"))
    assert(inside(ops, "S" -> "a", "a" -> "E", "a" -> "S") == Set("a"))
  }

  it should "end on the loop output fed back into the body, leaving the feedback operator outside" in {
    // src -> S -> a -> E -> b -> a: b is downstream of the LoopEnd; walking upstream from b,
    // E opens a block that S closes again, however many laps the walk takes.
    val ops = List(op("src"), start("S"), op("a"), end("E"), op("b"))
    val result = inside(ops, "src" -> "S", "S" -> "a", "a" -> "E", "E" -> "b", "b" -> "a")
    assert(result == Set("a"))
  }

  it should "end on a cycle through a LoopEnd that no LoopStart reaches" in {
    // S -> a -> E, and apart from it x -> E2 -> y -> x: walking upstream from x, every lap
    // passes E2 and opens one more block, and no LoopStart ever closes them.
    val ops = List(start("S"), op("a"), end("E"), op("x"), end("E2"), op("y"))
    val result = inside(ops, "S" -> "a", "a" -> "E", "x" -> "E2", "E2" -> "y", "y" -> "x")
    assert(result == Set("a"))
  }
}
