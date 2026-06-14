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

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class ReservoirSamplingOpExecSpec extends AnyFlatSpec {

  private val schema: Schema =
    Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(new Attribute("v", AttributeType.INTEGER), Integer.valueOf(v)).build()

  // A wider schema mixing every primitive attribute type, to prove sampling treats
  // tuples opaquely and preserves all fields regardless of arity or type.
  private val complexSchema: Schema =
    Schema()
      .add(new Attribute("id", AttributeType.INTEGER))
      .add(new Attribute("label", AttributeType.STRING))
      .add(new Attribute("score", AttributeType.DOUBLE))
      .add(new Attribute("flag", AttributeType.BOOLEAN))
      .add(new Attribute("big", AttributeType.LONG))

  private def complexTuple(i: Int): Tuple =
    Tuple
      .builder(complexSchema)
      .add(new Attribute("id", AttributeType.INTEGER), Integer.valueOf(i))
      .add(new Attribute("label", AttributeType.STRING), s"row-$i")
      .add(new Attribute("score", AttributeType.DOUBLE), java.lang.Double.valueOf(i * 1.5))
      .add(new Attribute("flag", AttributeType.BOOLEAN), java.lang.Boolean.valueOf(i % 2 == 0))
      .add(new Attribute("big", AttributeType.LONG), java.lang.Long.valueOf(i.toLong))
      .build()

  // LogicalOp is registered for polymorphic Jackson deserialization via the
  // `operatorType` discriminator, so a hand-rolled `{"k":N}` string would fail
  // to bind. Serialize a real `ReservoirSamplingOpDesc` to embed the discriminator.
  private def desc(k: Int): String = {
    val d = new ReservoirSamplingOpDesc()
    d.k = k
    objectMapper.writeValueAsString(d)
  }

  // `k` is renamed by @JsonProperty, so resolve the JSON key from the annotation
  // rather than hard-coding it, then overwrite that slot with null on a real desc.
  private def descWithNullK: String = {
    val node = objectMapper.valueToTree[ObjectNode](new ReservoirSamplingOpDesc())
    val keyForK =
      classOf[ReservoirSamplingOpDesc]
        .getDeclaredField("k")
        .getAnnotation(classOf[com.fasterxml.jackson.annotation.JsonProperty])
        .value()
    node.putNull(keyForK)
    objectMapper.writeValueAsString(node)
  }

  private def newExec(k: Int, idx: Int = 0, workerCount: Int = 1): ReservoirSamplingOpExec = {
    val exec = new ReservoirSamplingOpExec(desc(k), idx, workerCount)
    exec.open()
    exec
  }

  /** Feed every value through processTuple, then drain onFinish into a list. */
  private def runFinish(exec: ReservoirSamplingOpExec, values: Seq[Int]): List[Tuple] =
    runFinishTuples(exec, values.map(tuple))

  /** Feed pre-built tuples through processTuple, then drain onFinish into a list. */
  private def runFinishTuples(exec: ReservoirSamplingOpExec, tuples: Seq[Tuple]): List[Tuple] = {
    tuples.foreach(t => exec.processTuple(t, 0))
    exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList
  }

  "ReservoirSamplingOpExec.processTuple" should "buffer silently and emit nothing until onFinish" in {
    val exec = newExec(k = 3)
    val perTupleEmissions = (0 until 10).map(i => exec.processTuple(tuple(i), 0).toList)
    assert(
      perTupleEmissions.forall(_.isEmpty),
      "processTuple should never emit; sampling emits on finish"
    )
  }

  "ReservoirSamplingOpExec.onFinish" should "return all input tuples in order when input size == k" in {
    val exec = newExec(k = 4)
    val emitted = runFinish(exec, 0 until 4)
    assert(emitted == List(tuple(0), tuple(1), tuple(2), tuple(3)))
  }

  it should "emit only the filled prefix, without null padding, when input size < k" in {
    val exec = newExec(k = 5)
    val emitted = runFinish(exec, 0 until 2)
    assert(emitted == List(tuple(0), tuple(1)), "only the received tuples are emitted, in order")
    assert(!emitted.contains(null), "the unfilled reservoir slots must not leak as null tuples")
  }

  it should "emit nothing when the input stream is empty" in {
    val exec = newExec(k = 5)
    val emitted = runFinish(exec, Seq.empty)
    assert(emitted.isEmpty, "an unfilled reservoir with no input emits no (null) tuples")
  }

  it should "not emit null padding on a worker that receives fewer tuples than its share" in {
    // k=10 over 3 workers gives worker 0 a share of 4 (equallyPartitionGoal), but skewed
    // partitioning delivers it only 2 tuples; the 2 unfilled slots must not surface as nulls.
    val exec = newExec(k = 10, idx = 0, workerCount = 3)
    val emitted = runFinish(exec, 0 until 2)
    assert(emitted == List(tuple(0), tuple(1)))
  }

  it should "keep exactly k tuples, all drawn from the input, when input size > k" in {
    val exec = newExec(k = 5)
    val input = 0 until 100
    val emitted = runFinish(exec, input)

    assert(emitted.size == 5, "reservoir must hold exactly k samples")
    assert(!emitted.contains(null), "no null padding when the reservoir is fully filled")
    val inputTuples = input.map(tuple).toSet
    assert(
      emitted.forall(inputTuples.contains),
      "every sample must originate from the input stream"
    )
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

  it should "preserve every field of multi-attribute tuples drawn from the input (complex schema)" in {
    val exec = newExec(k = 5)
    val input = (0 until 100).map(complexTuple)
    val emitted = runFinishTuples(exec, input)

    assert(emitted.size == 5, "reservoir must hold exactly k samples")
    assert(!emitted.contains(null), "no null padding when the reservoir is fully filled")
    val inputTuples = input.toSet
    assert(
      emitted.forall(inputTuples.contains),
      "each sample is an intact input tuple with all five attributes preserved"
    )
    assert(emitted.distinct.size == emitted.size, "each input tuple is sampled at most once")
  }

  "ReservoirSamplingOpExec with a bad k" should "reject a negative k with a negative-sized reservoir" in {
    // equallyPartitionGoal(-1, 1) -> count = -1, so open() allocates Array.ofDim(-1).
    assertThrows[NegativeArraySizeException] {
      newExec(k = -1)
    }
  }

  it should "coerce a null k to 0 and reject sampling a populated stream" in {
    // A null k deserializes to the primitive default 0, yielding a zero-length
    // reservoir; the first replacement draw then calls Random.nextInt(0).
    val exec = new ReservoirSamplingOpExec(descWithNullK, 0, 1)
    exec.open()
    assert(exec.onFinish(0).isEmpty, "an empty reservoir emits nothing")
    assertThrows[IllegalArgumentException] {
      exec.processTuple(tuple(0), 0)
    }
  }
}
