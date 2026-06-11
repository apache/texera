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

package org.apache.texera.amber.engine.architecture.sendsemantics.partitioners

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  OneToOnePartitioning,
  RoundRobinPartitioning
}
import org.scalatest.flatspec.AnyFlatSpec

class PartitionersSpec extends AnyFlatSpec {

  private val sender: ActorVirtualIdentity = ActorVirtualIdentity("sender")
  private val r1: ActorVirtualIdentity = ActorVirtualIdentity("rec1")
  private val r2: ActorVirtualIdentity = ActorVirtualIdentity("rec2")
  private val r3: ActorVirtualIdentity = ActorVirtualIdentity("rec3")

  private def channel(to: ActorVirtualIdentity): ChannelIdentity =
    ChannelIdentity(sender, to, isControl = false)

  private val intAttr: Attribute = new Attribute("v", AttributeType.INTEGER)
  private val intSchema: Schema = Schema().add(intAttr)

  private def intTuple(value: Int): Tuple =
    Tuple.builder(intSchema).add(intAttr, value).build()

  private val twoStringSchema: Schema = Schema()
    .add(new Attribute("k", AttributeType.STRING))
    .add(new Attribute("v", AttributeType.STRING))

  private def stringTuple(k: String, v: String): Tuple =
    Tuple
      .builder(twoStringSchema)
      .add(new Attribute("k", AttributeType.STRING), k)
      .add(new Attribute("v", AttributeType.STRING), v)
      .build()

  // -- OneToOnePartitioner --------------------------------------------------

  "OneToOnePartitioner.getBucketIndex" should "always return Iterator(0)" in {
    val partitioning = OneToOnePartitioning(
      batchSize = 100,
      channels = Seq(channel(r1))
    )
    val partitioner = OneToOnePartitioner(partitioning, sender)
    assert(partitioner.getBucketIndex(intTuple(7)).toList == List(0))
    assert(partitioner.getBucketIndex(intTuple(42)).toList == List(0))
  }

  "OneToOnePartitioner.allReceivers" should "return the receiver from the channel matching the actor id" in {
    val partitioning = OneToOnePartitioning(
      batchSize = 100,
      channels = Seq(
        ChannelIdentity(ActorVirtualIdentity("other-sender"), r2, isControl = false),
        channel(r1)
      )
    )
    val partitioner = OneToOnePartitioner(partitioning, sender)
    assert(partitioner.allReceivers == Seq(r1))
  }

  // -- BroadcastPartitioner -------------------------------------------------

  "BroadcastPartitioner.getBucketIndex" should "yield every receiver index for any tuple" in {
    val partitioning = BroadcastPartitioning(
      batchSize = 100,
      channels = Seq(channel(r1), channel(r2), channel(r3))
    )
    val partitioner = BroadcastPartitioner(partitioning)
    assert(partitioner.getBucketIndex(intTuple(0)).toList == List(0, 1, 2))
  }

  "BroadcastPartitioner" should "deduplicate receivers when channels list a worker twice" in {
    val partitioning = BroadcastPartitioning(
      batchSize = 100,
      channels = Seq(channel(r1), channel(r1), channel(r2))
    )
    val partitioner = BroadcastPartitioner(partitioning)
    assert(partitioner.allReceivers == Seq(r1, r2))
    assert(partitioner.getBucketIndex(intTuple(0)).toList == List(0, 1))
  }

  // -- RoundRobinPartitioner ------------------------------------------------

  "RoundRobinPartitioner.getBucketIndex" should "cycle through bucket indices" in {
    val partitioning = RoundRobinPartitioning(
      batchSize = 100,
      channels = Seq(channel(r1), channel(r2), channel(r3))
    )
    val partitioner = RoundRobinPartitioner(partitioning)

    val indices = (1 to 7).map(_ => partitioner.getBucketIndex(intTuple(0)).next()).toList
    // Implementation increments first, then emits. Starting from 0, the first
    // emitted index is therefore 1, then 2, then 0, repeating.
    assert(indices == List(1, 2, 0, 1, 2, 0, 1))
  }

  "RoundRobinPartitioner.allReceivers" should "preserve channel order while deduplicating" in {
    val partitioning = RoundRobinPartitioning(
      batchSize = 100,
      channels = Seq(channel(r2), channel(r1), channel(r2))
    )
    val partitioner = RoundRobinPartitioner(partitioning)
    assert(partitioner.allReceivers == Seq(r2, r1))
  }

  // -- HashBasedShufflePartitioner ------------------------------------------

  "HashBasedShufflePartitioner.getBucketIndex" should "return a non-negative index within the receiver count" in {
    val partitioning = HashBasedShufflePartitioning(
      batchSize = 100,
      channels = Seq(channel(r1), channel(r2), channel(r3)),
      hashAttributeNames = Seq("k")
    )
    val partitioner = HashBasedShufflePartitioner(partitioning)

    (0 until 50).foreach { i =>
      val idx = partitioner.getBucketIndex(stringTuple(s"key-$i", "v")).next()
      assert(idx >= 0 && idx < 3, s"index $idx out of range for tuple key-$i")
    }
  }

  it should "be deterministic for the same input tuple" in {
    val partitioning = HashBasedShufflePartitioning(
      batchSize = 100,
      channels = Seq(channel(r1), channel(r2), channel(r3)),
      hashAttributeNames = Seq("k")
    )
    val partitioner = HashBasedShufflePartitioner(partitioning)

    // Same tuple instance, two consecutive calls — the contract says the
    // second call must produce the same bucket as the first.
    val tuple = stringTuple("alpha", "ignored")
    val first = partitioner.getBucketIndex(tuple).next()
    val second = partitioner.getBucketIndex(tuple).next()
    assert(first == second)
  }

  it should "depend only on the hash-attribute subset, not on other fields" in {
    val partitioning = HashBasedShufflePartitioning(
      batchSize = 100,
      channels = Seq(channel(r1), channel(r2), channel(r3)),
      hashAttributeNames = Seq("k")
    )
    val partitioner = HashBasedShufflePartitioner(partitioning)

    // Sweep several (k, v) pairs so a buggy implementation that hashes the
    // full tuple would have to collide modulo 3 on every single key — which
    // is not realistic for any reasonable hash. For each k, vary the second
    // field across multiple values; the bucket must be the same for all of
    // them.
    val keys = Seq("alpha", "beta", "gamma", "delta", "epsilon", "zeta")
    val varyingSecondField = (0 until 8).map(i => s"v-$i")
    keys.foreach { k =>
      val buckets =
        varyingSecondField.map(v => partitioner.getBucketIndex(stringTuple(k, v)).next())
      assert(
        buckets.distinct.size == 1,
        s"key=$k produced different buckets when varying the non-hash field: $buckets"
      )
    }
  }

  it should "use the full tuple when no hash attributes are configured" in {
    val partitioning = HashBasedShufflePartitioning(
      batchSize = 100,
      channels = Seq(channel(r1), channel(r2), channel(r3)),
      hashAttributeNames = Seq.empty
    )
    val partitioner = HashBasedShufflePartitioner(partitioning)

    // Hold k constant; vary the second field across many values. If the
    // partitioner hashed only the (empty) hash-attr subset, every bucket
    // would collapse to a single value. With the full tuple feeding the
    // hash, varying v across enough samples must produce more than one
    // distinct bucket among 3 receivers.
    val sampleSize = 50
    val buckets =
      (0 until sampleSize).map(i => partitioner.getBucketIndex(stringTuple("k", s"v-$i")).next())
    buckets.foreach(idx => assert(idx >= 0 && idx < 3))
    assert(
      buckets.distinct.size > 1,
      s"empty hashAttributeNames should hash the full tuple, but $sampleSize samples all landed in: ${buckets.distinct}"
    )
  }

  "HashBasedShufflePartitioner.allReceivers" should "deduplicate channel destinations" in {
    val partitioning = HashBasedShufflePartitioning(
      batchSize = 100,
      channels = Seq(channel(r1), channel(r2), channel(r1)),
      hashAttributeNames = Seq("k")
    )
    val partitioner = HashBasedShufflePartitioner(partitioning)
    assert(partitioner.allReceivers == Seq(r1, r2))
  }
}
