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

package org.apache.texera.amber.operator.reservoirsampling

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class ReservoirSamplingOpExecSpec extends AnyFlatSpec {

  private val schema: Schema =
    Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(new Attribute("v", AttributeType.INTEGER), Integer.valueOf(v)).build()

  // LogicalOp is registered for polymorphic Jackson deserialization via the
  // `operatorType` discriminator, so a hand-rolled `{"k":N}` string would fail
  // to bind. Serialize a real `ReservoirSamplingOpDesc` to embed the discriminator.
  private def desc(k: Int): String = {
    val d = new ReservoirSamplingOpDesc()
    d.k = k
    objectMapper.writeValueAsString(d)
  }

  private def newExec(k: Int, idx: Int = 0, workerCount: Int = 1): ReservoirSamplingOpExec = {
    val exec = new ReservoirSamplingOpExec(desc(k), idx, workerCount)
    exec.open()
    exec
  }

  /** Feed every value through processTuple, then drain onFinish into a list. */
  private def runFinish(exec: ReservoirSamplingOpExec, values: Seq[Int]): List[Tuple] = {
    values.foreach(v => exec.processTuple(tuple(v), 0))
    exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList
  }

  "ReservoirSamplingOpExec.processTuple" should "buffer silently and emit nothing until onFinish" in {
    val exec = newExec(k = 3)
    val perTupleEmissions = (0 until 10).map(i => exec.processTuple(tuple(i), 0).toList)
    assert(perTupleEmissions.forall(_.isEmpty), "processTuple should never emit; sampling emits on finish")
  }

  "ReservoirSamplingOpExec.onFinish" should "return all input tuples in order when input size == k" in {
    val exec = newExec(k = 4)
    val emitted = runFinish(exec, 0 until 4)
    assert(emitted == List(tuple(0), tuple(1), tuple(2), tuple(3)))
  }

  it should "keep exactly k tuples, all drawn from the input, when input size > k" in {
    val exec = newExec(k = 5)
    val input = 0 until 100
    val emitted = runFinish(exec, input)

    assert(emitted.size == 5, "reservoir must hold exactly k samples")
    assert(!emitted.contains(null), "no null padding when the reservoir is fully filled")
    val inputTuples = input.map(tuple).toSet
    assert(emitted.forall(inputTuples.contains), "every sample must originate from the input stream")
    assert(emitted.distinct.size == emitted.size, "each input tuple is sampled at most once")
  }

  it should "be deterministic across runs (RNG is seeded, so identical input yields identical samples)" in {
    val input = 0 until 100
    val firstRun = runFinish(newExec(k = 7), input)
    val secondRun = runFinish(newExec(k = 7), input)
    assert(firstRun == secondRun)
    // Sanity-check the sample is not simply the first k tuples, i.e. replacement happened.
    assert(firstRun != (0 until 7).map(tuple).toList)
  }

  it should "distribute k across workers via equallyPartitionGoal (k=10, 3 workers -> 4,3,3)" in {
    // The remainder is handed to the lowest-indexed workers, so worker 0 keeps one extra.
    val perWorkerSize = (0 until 3).map { idx =>
      runFinish(newExec(k = 10, idx = idx, workerCount = 3), 0 until 50).size
    }
    assert(perWorkerSize == Seq(4, 3, 3))
    assert(perWorkerSize.sum == 10, "the per-worker reservoirs together hold the requested k")
  }

  "ReservoirSamplingOpExec.open" should "reset state so a reused executor re-samples from scratch" in {
    val exec = newExec(k = 3)
    runFinish(exec, 0 until 20) // first pass consumes the executor's state
    exec.open() // reopen should clear n and the reservoir
    val emitted = runFinish(exec, Seq(100, 101, 102))
    assert(emitted == List(tuple(100), tuple(101), tuple(102)))
  }

  // Sharp edge worth a reviewer's attention: when fewer than k tuples arrive, the
  // fixed-size reservoir is never fully filled, so onFinish emits the buffered
  // tuples followed by null padding. Downstream operators receive null tuples.
  // This test documents the current behavior; emitting nulls is very likely a bug
  // (onFinish should probably be `reservoir.iterator.take(n)` / filter nulls).
  it should "currently emit null padding when input size < k (documents a likely bug)" in {
    val exec = newExec(k = 5)
    val emitted = runFinish(exec, 0 until 3)
    assert(emitted.size == 5)
    assert(emitted.take(3) == List(tuple(0), tuple(1), tuple(2)))
    assert(emitted.drop(3) == List(null, null), "trailing reservoir slots are emitted as null tuples")
  }
}
