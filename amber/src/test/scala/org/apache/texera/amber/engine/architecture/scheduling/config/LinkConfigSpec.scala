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

package org.apache.texera.amber.engine.architecture.scheduling.config

import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.{
  BroadcastPartition,
  HashPartition,
  OneToOnePartition,
  RangePartition,
  SinglePartition,
  UnknownPartition
}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  OneToOnePartitioning,
  RangeBasedShufflePartitioning,
  RoundRobinPartitioning
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LinkConfigSpec extends AnyFlatSpec with Matchers {

  private val w1 = ActorVirtualIdentity("w1")
  private val w2 = ActorVirtualIdentity("w2")
  private val w3 = ActorVirtualIdentity("w3")
  private val u1 = ActorVirtualIdentity("u1")
  private val u2 = ActorVirtualIdentity("u2")
  private val u3 = ActorVirtualIdentity("u3")
  private val batch = 64

  private def endpoints(channels: Seq[ChannelIdentity]): Seq[(String, String)] =
    channels.map(c => (c.fromWorkerId.name, c.toWorkerId.name))

  // ----- HashPartition -----

  "toPartitioning" should "produce a HashBasedShufflePartitioning with full cross product channels" in {
    val out = LinkConfig.toPartitioning(
      List(w1, w2),
      List(u1, u2, u3),
      HashPartition(List("k1", "k2")),
      batch
    )
    out shouldBe a[HashBasedShufflePartitioning]
    val hp = out.asInstanceOf[HashBasedShufflePartitioning]
    hp.batchSize shouldBe batch
    hp.hashAttributeNames shouldBe Seq("k1", "k2")
    endpoints(hp.channels) shouldBe Seq(
      ("w1", "u1"),
      ("w1", "u2"),
      ("w1", "u3"),
      ("w2", "u1"),
      ("w2", "u2"),
      ("w2", "u3")
    )
    hp.channels.foreach(_.isControl shouldBe false)
  }

  // ----- RangePartition -----

  "RangePartition" should "produce a RangeBasedShufflePartitioning carrying the range bounds and cross-product channels" in {
    val out = LinkConfig.toPartitioning(
      List(w1),
      List(u1, u2),
      RangePartition(List("k"), 0L, 100L),
      batch
    )
    out shouldBe a[RangeBasedShufflePartitioning]
    val rp = out.asInstanceOf[RangeBasedShufflePartitioning]
    rp.batchSize shouldBe batch
    rp.rangeAttributeNames shouldBe Seq("k")
    rp.rangeMin shouldBe 0L
    rp.rangeMax shouldBe 100L
    endpoints(rp.channels) shouldBe Seq(("w1", "u1"), ("w1", "u2"))
  }

  // ----- SinglePartition -----

  "SinglePartition" should "produce a OneToOnePartitioning with one channel per from-worker to the single to-worker" in {
    val out = LinkConfig.toPartitioning(
      List(w1, w2, w3),
      List(u1),
      SinglePartition(),
      batch
    )
    out shouldBe a[OneToOnePartitioning]
    val op = out.asInstanceOf[OneToOnePartitioning]
    op.batchSize shouldBe batch
    endpoints(op.channels) shouldBe Seq(("w1", "u1"), ("w2", "u1"), ("w3", "u1"))
  }

  it should "raise an AssertionError when more than one to-worker is supplied" in {
    assertThrows[AssertionError] {
      LinkConfig.toPartitioning(List(w1, w2), List(u1, u2), SinglePartition(), batch)
    }
  }

  // ----- OneToOnePartition -----

  "OneToOnePartition" should "produce a OneToOnePartitioning with zip pairing for equal-length inputs" in {
    val out = LinkConfig.toPartitioning(
      List(w1, w2, w3),
      List(u1, u2, u3),
      OneToOnePartition(),
      batch
    )
    out shouldBe a[OneToOnePartitioning]
    val op = out.asInstanceOf[OneToOnePartitioning]
    endpoints(op.channels) shouldBe Seq(("w1", "u1"), ("w2", "u2"), ("w3", "u3"))
  }

  it should "silently truncate when from and to lengths differ (current behavior)" in {
    // Pin: same `List.zip` truncation hazard as ChannelConfig (Bug #4799).
    // Documenting the parallel here so a fix that aligns the two helpers
    // surfaces this spec at the same time.
    val out = LinkConfig.toPartitioning(
      List(w1, w2, w3),
      List(u1, u2),
      OneToOnePartition(),
      batch
    )
    val op = out.asInstanceOf[OneToOnePartitioning]
    endpoints(op.channels) shouldBe Seq(("w1", "u1"), ("w2", "u2"))
  }

  // ----- BroadcastPartition -----

  "BroadcastPartition" should "produce a BroadcastPartitioning whose channels follow zip pairing today (current behavior)" in {
    // Pin: BroadcastPartition currently uses `fromWorkerIds.zip(toWorkerIds)`
    // — the SAME 1:1 pairing as OneToOnePartition. ChannelConfig in the same
    // package emits a full cross product for the BroadcastPartition arm,
    // which matches broadcast semantics ("each sender targets every
    // receiver"). The two helpers diverge today; pinning this so a fix that
    // realigns the contract surfaces here. Filed as a Bug.
    val out = LinkConfig.toPartitioning(
      List(w1, w2, w3),
      List(u1, u2, u3),
      BroadcastPartition(),
      batch
    )
    out shouldBe a[BroadcastPartitioning]
    val bp = out.asInstanceOf[BroadcastPartitioning]
    bp.batchSize shouldBe batch
    endpoints(bp.channels) shouldBe Seq(("w1", "u1"), ("w2", "u2"), ("w3", "u3"))
  }

  it should "silently truncate broadcast pairings when sides differ in length (current behavior)" in {
    val out = LinkConfig.toPartitioning(
      List(w1, w2, w3),
      List(u1, u2),
      BroadcastPartition(),
      batch
    )
    val bp = out.asInstanceOf[BroadcastPartitioning]
    endpoints(bp.channels) shouldBe Seq(("w1", "u1"), ("w2", "u2"))
  }

  // ----- UnknownPartition -----

  "UnknownPartition" should "produce a RoundRobinPartitioning with the full cross product" in {
    val out = LinkConfig.toPartitioning(
      List(w1, w2),
      List(u1, u2),
      UnknownPartition(),
      batch
    )
    out shouldBe a[RoundRobinPartitioning]
    val rr = out.asInstanceOf[RoundRobinPartitioning]
    rr.batchSize shouldBe batch
    endpoints(rr.channels) shouldBe Seq(
      ("w1", "u1"),
      ("w1", "u2"),
      ("w2", "u1"),
      ("w2", "u2")
    )
  }

  // ----- empty inputs -----

  // The previous block ended with a `"UnknownPartition" should ...` subject.
  // Switch back to "toPartitioning" so test reports for the empty-input,
  // batch-propagation, and unsupported-branch cases below don't get
  // misattributed to UnknownPartition.
  "toPartitioning" should "return empty channels when fromWorkerIds is empty (cross-product arm)" in {
    val out = LinkConfig.toPartitioning(
      Nil,
      List(u1, u2),
      HashPartition(),
      batch
    )
    out.asInstanceOf[HashBasedShufflePartitioning].channels shouldBe empty
  }

  it should "return empty channels when toWorkerIds is empty (cross-product arm)" in {
    val out = LinkConfig.toPartitioning(
      List(w1, w2),
      Nil,
      HashPartition(),
      batch
    )
    out.asInstanceOf[HashBasedShufflePartitioning].channels shouldBe empty
  }

  // ----- batch size propagation -----

  it should "propagate dataTransferBatchSize verbatim regardless of partitioning arm" in {
    val customBatch = 1024
    val out = LinkConfig.toPartitioning(
      List(w1),
      List(u1),
      OneToOnePartition(),
      customBatch
    )
    out.asInstanceOf[OneToOnePartitioning].batchSize shouldBe customBatch
  }

  // ----- unsupported branch -----

  it should "throw UnsupportedOperationException when partitionInfo is unrecognized" in {
    // PartitionInfo is sealed, so the only way to reach the catch-all
    // `case _` branch from a test is to pass an off-domain value such as
    // null. This pins the contract that an unknown PartitionInfo subtype
    // results in UnsupportedOperationException rather than silently
    // dropping into a default partitioning.
    assertThrows[UnsupportedOperationException] {
      LinkConfig.toPartitioning(
        List(w1),
        List(u1),
        null,
        batch
      )
    }
  }
}
