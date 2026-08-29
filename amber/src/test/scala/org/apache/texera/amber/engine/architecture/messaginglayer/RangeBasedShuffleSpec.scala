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

package org.apache.texera.amber.engine.architecture.messaginglayer

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitioners.RangeBasedShufflePartitioner
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.RangeBasedShufflePartitioning
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class RangeBasedShuffleSpec extends AnyFlatSpec with MockFactory {
  val identifier = ActorVirtualIdentity("batch producer mock")
  val fakeID1: ActorVirtualIdentity = ActorVirtualIdentity("rec1")
  val fakeID2: ActorVirtualIdentity = ActorVirtualIdentity("rec2")
  val fakeID3: ActorVirtualIdentity = ActorVirtualIdentity("rec3")
  val fakeID4: ActorVirtualIdentity = ActorVirtualIdentity("rec4")
  val fakeID5: ActorVirtualIdentity = ActorVirtualIdentity("rec5")

  val attr: Attribute = new Attribute("Attr1", AttributeType.INTEGER)
  val schema: Schema = Schema().add(attr)
  val partitioning: RangeBasedShufflePartitioning =
    RangeBasedShufflePartitioning(
      400,
      List(
        ChannelIdentity(identifier, fakeID1, isControl = false),
        ChannelIdentity(identifier, fakeID2, isControl = false),
        ChannelIdentity(identifier, fakeID3, isControl = false),
        ChannelIdentity(identifier, fakeID4, isControl = false),
        ChannelIdentity(identifier, fakeID5, isControl = false)
      ),
      Seq("Attr1"),
      -400,
      600
    )

  val partitioner: RangeBasedShufflePartitioner = RangeBasedShufflePartitioner(partitioning)

  "RangeBasedShuffleSpec" should "return 0 when value is less than rangeMin" in {
    val tuple = Tuple.builder(schema).add(attr, -600).build()
    val idx = partitioner.getBucketIndex(tuple)
    // `toList`, not `next()`: range partitioning routes each tuple to exactly one bucket. The
    // `Iterator[Int]` return type is shared with BroadcastPartitioner, which legitimately yields
    // many, so "exactly one" is this partitioner's contract and not the type's.
    assert(idx.toList == List(0))

    // -600 sits in the one window where the clamp and the raw arithmetic agree: Long division
    // truncates toward zero, so (-600 - -400) / 201 is 0 with or without the guard. -1000 is
    // outside it -- unclamped the arithmetic yields -2, and `OutputManager.passTupleToDownstream`
    // feeds the bucket index straight into `partitioner.allReceivers(...)`.
    val farBelow = Tuple.builder(schema).add(attr, -1000).build()
    assert(partitioner.getBucketIndex(farBelow).toList == List(0))
  }

  "RangeBasedShuffleSpec" should "return last receiver when value is more than rangeMax" in {
    val tuple = Tuple.builder(schema).add(attr, 800).build()
    val idx = partitioner.getBucketIndex(tuple)
    assert(idx.toList == List(4))
  }

  "RangeBasedShuffleSpec" should "find index correctly" in {
    var tuple = Tuple.builder(schema).add(attr, -400).build()
    var idx = partitioner.getBucketIndex(tuple)
    assert(idx.toList == List(0))

    tuple = Tuple.builder(schema).add(attr, -200).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx.toList == List(0))

    tuple = Tuple.builder(schema).add(attr, -199).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx.toList == List(1))
  }

  "RangeBasedShuffleSpec" should "handle different data types correctly" in {
    var tuple = Tuple.builder(schema).add(attr, -90).build()
    var idx = partitioner.getBucketIndex(tuple)
    assert(idx.toList == List(1))

    val partitioning2: RangeBasedShufflePartitioning =
      RangeBasedShufflePartitioning(
        400,
        List(
          ChannelIdentity(identifier, fakeID1, isControl = false),
          ChannelIdentity(identifier, fakeID2, isControl = false),
          ChannelIdentity(identifier, fakeID3, isControl = false),
          ChannelIdentity(identifier, fakeID4, isControl = false),
          ChannelIdentity(identifier, fakeID5, isControl = false)
        ),
        Seq("Attr2"),
        -400,
        600
      )

    val partitioner2: RangeBasedShufflePartitioner = RangeBasedShufflePartitioner(partitioning2)
    val doubleAttr: Attribute = new Attribute("Attr2", AttributeType.DOUBLE)
    val doubleSchema: Schema = Schema().add(doubleAttr)
    tuple = Tuple.builder(doubleSchema).add(doubleAttr, -90.5).build()
    idx = partitioner2.getBucketIndex(tuple)
    assert(idx.toList == List(1))

    val partitioning3: RangeBasedShufflePartitioning =
      RangeBasedShufflePartitioning(
        400,
        List(
          ChannelIdentity(identifier, fakeID1, isControl = false),
          ChannelIdentity(identifier, fakeID2, isControl = false),
          ChannelIdentity(identifier, fakeID3, isControl = false),
          ChannelIdentity(identifier, fakeID4, isControl = false),
          ChannelIdentity(identifier, fakeID5, isControl = false)
        ),
        Seq("Attr3"),
        -400,
        600
      )

    val partitioner3: RangeBasedShufflePartitioner = RangeBasedShufflePartitioner(partitioning3)
    val longAttr: Attribute = new Attribute("Attr3", AttributeType.LONG)
    val longSchema: Schema = Schema().add(longAttr)
    tuple = Tuple.builder(longSchema).add(longAttr, -90L).build()
    idx = partitioner3.getBucketIndex(tuple)
    assert(idx.toList == List(1))
  }

  "RangeBasedShuffleSpec" should "refuse a range attribute it cannot widen to a Long" in {
    // Only LONG / INTEGER / DOUBLE reach the bucket arithmetic. Anything else has to fail loudly:
    // `fieldVal` starts at -1, so a silently skipped type would route every tuple of that column
    // to bucket 0 (below rangeMin) instead of spreading it, and the sort a RangePartition exists
    // to enable would be wrong rather than absent.
    val stringAttr: Attribute = new Attribute("Attr4", AttributeType.STRING)
    // Two range attributes, the unsupported one FIRST. Only `rangeAttributeNames.head` is
    // consulted, so a partitioner that reached for any other name would find the INTEGER column
    // and bucket the tuple instead of refusing it -- which for the one production declarer
    // (SortPartitionsOpDesc) means routing by an unintended column rather than failing.
    val stringSchema: Schema = Schema().add(stringAttr).add(attr)
    val partitioning4: RangeBasedShufflePartitioning =
      RangeBasedShufflePartitioning(
        400,
        List(
          ChannelIdentity(identifier, fakeID1, isControl = false),
          ChannelIdentity(identifier, fakeID2, isControl = false)
        ),
        Seq("Attr4", "Attr1"),
        -400,
        600
      )
    val partitioner4: RangeBasedShufflePartitioner = RangeBasedShufflePartitioner(partitioning4)
    val tuple = Tuple.builder(stringSchema).add(stringAttr, "100").add(attr, 5).build()

    val failure = intercept[RuntimeException](partitioner4.getBucketIndex(tuple))

    // The message names the offending *type*, not the attribute: "Attr4" would not tell the user
    // what about the column is unsupported.
    assert(failure.getMessage == "unsupported attribute type: string")
  }

  "RangeBasedShuffleSpec" should "collapse a receiver its channels name twice" in {
    // Two channels can land on the same worker. `allReceivers` is what the bucket index is an
    // index *into*, and its size is also the divisor behind the bucket width, so keeping the
    // duplicate would both mis-address a bucket and shrink every bucket.
    val duplicated: RangeBasedShufflePartitioning =
      RangeBasedShufflePartitioning(
        400,
        List(
          ChannelIdentity(identifier, fakeID1, isControl = false),
          ChannelIdentity(identifier, fakeID2, isControl = false),
          ChannelIdentity(identifier, fakeID1, isControl = false),
          ChannelIdentity(identifier, fakeID3, isControl = false)
        ),
        Seq("Attr1"),
        -400,
        600
      )
    val deduplicating: RangeBasedShufflePartitioner = RangeBasedShufflePartitioner(duplicated)

    // First occurrence wins and channel order is preserved.
    assert(deduplicating.allReceivers == Seq(fakeID1, fakeID2, fakeID3))
    // ...and the bucket width follows the deduplicated count: 3 receivers over [-400, 600] give
    // 334 keys each, so -100 is still in the first bucket. With the duplicate counted the width
    // would be 251 and -100 would land in the second.
    assert(
      deduplicating.getBucketIndex(Tuple.builder(schema).add(attr, -100).build()).toList == List(0)
    )
    // ...and so does the one place the receiver *count* addresses a bucket rather than sizing one:
    // the above-rangeMax clamp returns the last index, which is 2 for the three deduplicated
    // receivers and would be 3 -- past the end of `allReceivers` -- for the four raw channels.
    assert(
      deduplicating.getBucketIndex(Tuple.builder(schema).add(attr, 800).build()).toList == List(2)
    )
  }

}
