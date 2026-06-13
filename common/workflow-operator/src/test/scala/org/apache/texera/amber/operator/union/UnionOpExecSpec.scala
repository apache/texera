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

package org.apache.texera.amber.operator.union

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

class UnionOpExecSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixture builders
  // ---------------------------------------------------------------------------

  private val attr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(attr)
  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(attr, Integer.valueOf(v)).build()

  // ---------------------------------------------------------------------------
  // Pass-through semantics
  // ---------------------------------------------------------------------------

  "UnionOpExec.processTuple" should
    "yield a single-element iterator containing the input tuple" in {
    val exec = new UnionOpExec
    val t = tuple(42)
    val out = exec.processTuple(t, port = 0).toList
    assert(out == List(t))
  }

  it should "preserve the exact Tuple instance (pass-through, no copy)" in {
    val exec = new UnionOpExec
    val t = tuple(7)
    val out = exec.processTuple(t, port = 0).toList
    assert(out.size == 1)
    assert(out.head eq t, "pass-through must return the same Tuple reference")
  }

  // ---------------------------------------------------------------------------
  // Port-agnostic behavior — union merges streams regardless of port id
  // ---------------------------------------------------------------------------

  it should "yield the same tuple regardless of which input port it arrived on" in {
    val exec = new UnionOpExec
    val t = tuple(1)
    val portsTested = List(0, 1, 5, 99, Int.MaxValue, -1)
    portsTested.foreach { p =>
      assert(exec.processTuple(t, port = p).toList == List(t), s"port=$p must pass through")
    }
  }

  // ---------------------------------------------------------------------------
  // Repeated calls — no state leakage
  // ---------------------------------------------------------------------------

  it should "return an independent fresh iterator on each call (no shared cursor)" in {
    val exec = new UnionOpExec
    val a = tuple(1)
    val b = tuple(2)
    val itA = exec.processTuple(a, port = 0)
    val itB = exec.processTuple(b, port = 1)
    // Consume a before b — neither call should affect the other.
    assert(itA.toList == List(a))
    assert(itB.toList == List(b))
  }

  it should "produce exactly one element per processTuple call" in {
    val exec = new UnionOpExec
    val t = tuple(1)
    val iter = exec.processTuple(t, port = 0)
    assert(iter.hasNext)
    iter.next()
    assert(!iter.hasNext, "iterator must be exhausted after the single pass-through")
  }

  // ---------------------------------------------------------------------------
  // Null tuple — pass-through is unconditional
  // ---------------------------------------------------------------------------

  it should "pass-through a null tuple unchanged (the impl does not null-check)" in {
    // Pin current behavior: `Iterator(tuple)` with `tuple = null` yields
    // an iterator containing `null`. If a future change adds a null-
    // check, that's a behavior change worth catching.
    val exec = new UnionOpExec
    val out = exec.processTuple(null, port = 0).toList
    assert(out == List(null))
  }

  // ---------------------------------------------------------------------------
  // Type contract — UnionOpExec is an OperatorExecutor
  // ---------------------------------------------------------------------------

  "UnionOpExec" should "be an OperatorExecutor (compile-time enforced)" in {
    val exec: org.apache.texera.amber.core.executor.OperatorExecutor = new UnionOpExec
    assert(exec.processTuple(tuple(1), port = 0).toList.size == 1)
  }
}
