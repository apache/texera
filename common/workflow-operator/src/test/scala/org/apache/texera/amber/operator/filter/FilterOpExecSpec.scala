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

package org.apache.texera.amber.operator.filter

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

class FilterOpExecSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Test harness — FilterOpExec is abstract. The existing
  // SpecializedFilterOpExecSpec covers the full SpecializedFilterOpExec
  // (predicate parsing + many AttributeType cases); here we exercise the
  // base trait's contract directly via a minimal concrete subclass that
  // exposes setFilterFunc.
  // ---------------------------------------------------------------------------

  private class BareFilterOpExec extends FilterOpExec

  // A one-attribute tuple is enough to drive predicate evaluation.
  private val attr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(attr)
  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(attr, Integer.valueOf(v)).build()

  // ---------------------------------------------------------------------------
  // processTuple — pass-through when predicate matches
  // ---------------------------------------------------------------------------

  "FilterOpExec.processTuple" should
    "yield the input tuple when filterFunc returns true" in {
    val exec = new BareFilterOpExec
    exec.setFilterFunc(_ => true)
    val t = tuple(42)
    val out = exec.processTuple(t, port = 0).toList
    assert(out == List(t))
  }

  it should "yield an empty Iterator when filterFunc returns false" in {
    val exec = new BareFilterOpExec
    exec.setFilterFunc(_ => false)
    val out = exec.processTuple(tuple(42), port = 0).toList
    assert(out.isEmpty)
  }

  it should "evaluate filterFunc against the actual tuple (not a copy or null)" in {
    // Capture what the predicate sees — must equal the argument passed in.
    val exec = new BareFilterOpExec
    var seen: Tuple = null
    exec.setFilterFunc { t =>
      seen = t
      true
    }
    val t = tuple(7)
    val _ = exec.processTuple(t, port = 0).toList
    assert(seen eq t, "filterFunc must receive the same Tuple instance")
  }

  it should "ignore the port argument (port-agnostic by contract)" in {
    val exec = new BareFilterOpExec
    exec.setFilterFunc(_ => true)
    val t = tuple(1)
    val portsTested = List(0, 1, 7, Int.MaxValue, -1)
    portsTested.foreach { p =>
      assert(exec.processTuple(t, port = p).toList == List(t), s"port=$p")
    }
  }

  it should "produce an iterator with exactly one element when the predicate matches" in {
    // The contract is "single tuple" — not a multi-element iterator.
    val exec = new BareFilterOpExec
    exec.setFilterFunc(_ => true)
    val iter = exec.processTuple(tuple(1), port = 0)
    assert(iter.hasNext)
    iter.next()
    assert(!iter.hasNext, "iterator must be exhausted after the single match")
  }

  // ---------------------------------------------------------------------------
  // setFilterFunc — swapping the predicate
  // ---------------------------------------------------------------------------

  "FilterOpExec.setFilterFunc" should
    "swap the predicate for subsequent processTuple calls" in {
    val exec = new BareFilterOpExec
    exec.setFilterFunc(_ => true)
    assert(exec.processTuple(tuple(1), port = 0).toList.size == 1)
    exec.setFilterFunc(_ => false)
    assert(exec.processTuple(tuple(1), port = 0).toList.isEmpty)
  }

  it should "accept a value-aware predicate that branches on the tuple's content" in {
    // Pin that the predicate is genuinely consulted per-tuple (not memoized).
    val exec = new BareFilterOpExec
    exec.setFilterFunc(t => t.getField[Integer](0).intValue() % 2 == 0)
    assert(exec.processTuple(tuple(2), port = 0).toList == List(tuple(2)))
    assert(exec.processTuple(tuple(3), port = 0).toList.isEmpty)
    assert(exec.processTuple(tuple(4), port = 0).toList == List(tuple(4)))
  }

  // ---------------------------------------------------------------------------
  // Serializable conformance — FilterOpExec extends Serializable so
  // executors can ship over the wire.
  // ---------------------------------------------------------------------------

  "FilterOpExec" should "be a Serializable OperatorExecutor (compile-time enforced)" in {
    val exec: java.io.Serializable = new BareFilterOpExec
    assert(exec != null)
    val asOpExec: org.apache.texera.amber.core.executor.OperatorExecutor =
      new BareFilterOpExec
    assert(asOpExec != null)
  }
}
