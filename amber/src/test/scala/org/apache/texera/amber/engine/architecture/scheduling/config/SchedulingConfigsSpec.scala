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

import org.apache.texera.amber.config.ApplicationConfig
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings._
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI

class SchedulingConfigsSpec extends AnyFlatSpec {

  private def actor(name: String): ActorVirtualIdentity = ActorVirtualIdentity(name)
  private def chan(from: ActorVirtualIdentity, to: ActorVirtualIdentity): ChannelIdentity =
    ChannelIdentity(from, to, isControl = false)

  // ---------------------------------------------------------------------------
  // ChannelConfig.generateChannelConfigs
  // ---------------------------------------------------------------------------

  "ChannelConfig.generateChannelConfigs" should "produce a full cross-product for HashPartition" in {
    val from = List(actor("f1"), actor("f2"))
    val to = List(actor("t1"), actor("t2"), actor("t3"))
    val configs =
      ChannelConfig.generateChannelConfigs(from, to, PortIdentity(0), HashPartition(List("k")))
    assert(configs.size == 6)
    assert(configs.map(_.channelId).toSet == (for (f <- from; t <- to) yield chan(f, t)).toSet)
    configs.foreach(c => assert(c.toPortId == PortIdentity(0)))
  }

  it should "produce a full cross-product for RangePartition" in {
    val from = List(actor("f1"))
    val to = List(actor("t1"), actor("t2"))
    val configs = ChannelConfig.generateChannelConfigs(
      from,
      to,
      PortIdentity(1),
      new RangePartition(List("k"), 0L, 10L)
    )
    assert(configs.size == 2)
  }

  it should "produce a full cross-product for BroadcastPartition" in {
    val from = List(actor("f1"), actor("f2"))
    val to = List(actor("t1"), actor("t2"))
    val configs =
      ChannelConfig.generateChannelConfigs(from, to, PortIdentity(0), BroadcastPartition())
    assert(configs.size == 4)
  }

  it should "produce a full cross-product for UnknownPartition" in {
    val from = List(actor("f1"))
    val to = List(actor("t1"), actor("t2"))
    val configs =
      ChannelConfig.generateChannelConfigs(from, to, PortIdentity(0), UnknownPartition())
    assert(configs.size == 2)
  }

  it should "fan-in to a single receiver for SinglePartition" in {
    val from = List(actor("f1"), actor("f2"), actor("f3"))
    val to = List(actor("only-receiver"))
    val configs =
      ChannelConfig.generateChannelConfigs(from, to, PortIdentity(0), SinglePartition())
    assert(configs.size == 3)
    assert(configs.forall(_.channelId.toWorkerId == actor("only-receiver")))
  }

  it should "fail the SinglePartition assertion when toWorkerIds has more than one entry" in {
    val from = List(actor("f1"))
    val to = List(actor("t1"), actor("t2"))
    assertThrows[AssertionError] {
      ChannelConfig.generateChannelConfigs(from, to, PortIdentity(0), SinglePartition())
    }
  }

  it should "zip from/to in OneToOnePartition" in {
    val from = List(actor("f1"), actor("f2"), actor("f3"))
    val to = List(actor("t1"), actor("t2"), actor("t3"))
    val configs =
      ChannelConfig.generateChannelConfigs(from, to, PortIdentity(0), OneToOnePartition())
    assert(configs.size == 3)
    val pairs = configs.map(c => (c.channelId.fromWorkerId, c.channelId.toWorkerId))
    assert(
      pairs == List(
        (actor("f1"), actor("t1")),
        (actor("f2"), actor("t2")),
        (actor("f3"), actor("t3"))
      )
    )
  }

  it should "produce empty list for unhandled partition cases" in {
    // PartitionInfo is sealed, so `null` is the only value that falls through
    // the named cases without adding a new subtype. This pins the catch-all
    // `case _ => List()` branch.
    val configs = ChannelConfig.generateChannelConfigs(
      List(actor("f")),
      List(actor("t")),
      PortIdentity(0),
      null.asInstanceOf[PartitionInfo]
    )
    assert(configs.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // LinkConfig.toPartitioning
  // ---------------------------------------------------------------------------

  "LinkConfig.toPartitioning" should "map HashPartition to HashBasedShufflePartitioning carrying its hash attributes" in {
    val from = List(actor("f"))
    val to = List(actor("t1"), actor("t2"))
    val partitioning =
      LinkConfig.toPartitioning(from, to, HashPartition(List("a", "b")), dataTransferBatchSize = 50)
    val hashed = partitioning.asInstanceOf[HashBasedShufflePartitioning]
    assert(hashed.batchSize == 50)
    assert(hashed.hashAttributeNames == List("a", "b"))
    assert(hashed.channels.size == 2)
  }

  it should "map RangePartition to RangeBasedShufflePartitioning carrying its range bounds" in {
    val from = List(actor("f"))
    val to = List(actor("t1"))
    val partitioning = LinkConfig.toPartitioning(
      from,
      to,
      new RangePartition(List("a"), 0L, 99L),
      dataTransferBatchSize = 10
    )
    val ranged = partitioning.asInstanceOf[RangeBasedShufflePartitioning]
    assert(ranged.batchSize == 10)
    assert(ranged.rangeMin == 0L)
    assert(ranged.rangeMax == 99L)
    assert(ranged.rangeAttributeNames == List("a"))
  }

  it should "map SinglePartition to OneToOnePartitioning fanned in to the single receiver" in {
    val from = List(actor("f1"), actor("f2"))
    val to = List(actor("only"))
    val partitioning =
      LinkConfig.toPartitioning(from, to, SinglePartition(), dataTransferBatchSize = 1)
    val one = partitioning.asInstanceOf[OneToOnePartitioning]
    assert(one.channels.forall(_.toWorkerId == actor("only")))
    assert(one.channels.size == 2)
  }

  it should "fail the SinglePartition assertion when toWorkerIds has more than one entry" in {
    val from = List(actor("f"))
    val to = List(actor("t1"), actor("t2"))
    assertThrows[AssertionError] {
      LinkConfig.toPartitioning(from, to, SinglePartition(), dataTransferBatchSize = 1)
    }
  }

  it should "map OneToOnePartition to OneToOnePartitioning over zipped pairs" in {
    val from = List(actor("f1"), actor("f2"))
    val to = List(actor("t1"), actor("t2"))
    val partitioning =
      LinkConfig.toPartitioning(from, to, OneToOnePartition(), dataTransferBatchSize = 1)
    val one = partitioning.asInstanceOf[OneToOnePartitioning]
    assert(one.channels.size == 2)
    assert(one.channels.head == chan(actor("f1"), actor("t1")))
  }

  it should "map BroadcastPartition to BroadcastPartitioning over zipped pairs" in {
    val from = List(actor("f1"), actor("f2"))
    val to = List(actor("t1"), actor("t2"))
    val partitioning =
      LinkConfig.toPartitioning(from, to, BroadcastPartition(), dataTransferBatchSize = 1)
    assert(partitioning.isInstanceOf[BroadcastPartitioning])
  }

  it should "map UnknownPartition to RoundRobinPartitioning across the cross-product" in {
    val from = List(actor("f1"), actor("f2"))
    val to = List(actor("t1"), actor("t2"))
    val partitioning =
      LinkConfig.toPartitioning(from, to, UnknownPartition(), dataTransferBatchSize = 1)
    val rr = partitioning.asInstanceOf[RoundRobinPartitioning]
    assert(rr.channels.size == 4)
  }

  it should "throw UnsupportedOperationException for unhandled partition cases" in {
    // PartitionInfo is sealed; `null` is the only value that falls through
    // the named cases without adding a new subtype. This pins the catch-all
    // `case _ => throw new UnsupportedOperationException()` branch.
    assertThrows[UnsupportedOperationException] {
      LinkConfig.toPartitioning(
        List(actor("f")),
        List(actor("t")),
        null.asInstanceOf[PartitionInfo],
        dataTransferBatchSize = 1
      )
    }
  }

  // ---------------------------------------------------------------------------
  // PortConfig hierarchy
  // ---------------------------------------------------------------------------

  "OutputPortConfig" should "expose its single storage URI via storageURIs" in {
    val uri = new URI("vfs:///wid/1/eid/1/result")
    val cfg = OutputPortConfig(uri)
    assert(cfg.storageURIs == List(uri))
  }

  "IntermediateInputPortConfig" should "expose every URI it was constructed with" in {
    val uris = List(new URI("vfs:///a"), new URI("vfs:///b"))
    val cfg = IntermediateInputPortConfig(uris)
    assert(cfg.storageURIs == uris)
  }

  "InputPortConfig" should "expose the URI projection of its storage pairs in order" in {
    val a = new URI("vfs:///a")
    val b = new URI("vfs:///b")
    val partitioningA = OneToOnePartitioning(1, Seq.empty)
    val partitioningB = OneToOnePartitioning(2, Seq.empty)
    val cfg = InputPortConfig(List((a, partitioningA), (b, partitioningB)))
    assert(cfg.storageURIs == List(a, b))
  }

  // ---------------------------------------------------------------------------
  // OperatorConfig
  // ---------------------------------------------------------------------------

  "OperatorConfig.empty" should "have no worker configs" in {
    assert(OperatorConfig.empty.workerConfigs.isEmpty)
  }

  it should "preserve the workerConfigs given at construction" in {
    val configs = List(WorkerConfig(actor("w1")), WorkerConfig(actor("w2")))
    val op = OperatorConfig(configs)
    assert(op.workerConfigs == configs)
  }

  // ---------------------------------------------------------------------------
  // ResourceConfig defaults
  // ---------------------------------------------------------------------------

  "ResourceConfig" should "default all three maps to empty" in {
    val rc = ResourceConfig()
    assert(rc.operatorConfigs.isEmpty)
    assert(rc.linkConfigs.isEmpty)
    assert(rc.portConfigs.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // WorkerConfig.generateWorkerConfigs
  // ---------------------------------------------------------------------------

  private def physicalOp(parallelizable: Boolean, suggested: Option[Int]): PhysicalOp =
    PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity("op"), "main"),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty,
      parallelizable = parallelizable,
      suggestedWorkerNum = suggested
    )

  "WorkerConfig.generateWorkerConfigs" should "produce exactly one WorkerConfig for non-parallelizable ops" in {
    val configs =
      WorkerConfig.generateWorkerConfigs(physicalOp(parallelizable = false, suggested = None))
    assert(configs.size == 1)
  }

  it should "ignore a suggested worker count for non-parallelizable ops" in {
    val configs =
      WorkerConfig.generateWorkerConfigs(physicalOp(parallelizable = false, suggested = Some(8)))
    assert(configs.size == 1)
  }

  it should "honor the suggested worker count for parallelizable ops" in {
    val configs =
      WorkerConfig.generateWorkerConfigs(physicalOp(parallelizable = true, suggested = Some(5)))
    assert(configs.size == 5)
    // distinct worker ids
    assert(configs.map(_.workerId).distinct.size == 5)
  }

  it should "fall back to the configured default when no suggested count is given for a parallelizable op" in {
    val configs =
      WorkerConfig.generateWorkerConfigs(physicalOp(parallelizable = true, suggested = None))
    assert(configs.size == ApplicationConfig.numWorkerPerOperatorByDefault)
  }
}
