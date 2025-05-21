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

package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.core.workflow.{
  BroadcastPartition,
  HashPartition,
  OneToOnePartition,
  PartitionInfo,
  RangePartition,
  SinglePartition,
  UnknownPartition
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings._
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

case object LinkConfig {
  def toPartitioning(
      fromWorkerIds: List[ActorVirtualIdentity],
      toWorkerIds: List[ActorVirtualIdentity],
      partitionInfo: PartitionInfo,
      dataTransferBatchSize: Int
  ): Partitioning = {
    partitionInfo match {
      case HashPartition(hashAttributeNames) =>
        HashBasedShufflePartitioning(
          dataTransferBatchSize,
          fromWorkerIds.flatMap(from =>
            toWorkerIds.map(to => ChannelIdentity(from, to, isControl = false))
          ),
          hashAttributeNames
        )

      case RangePartition(rangeAttributeNames, rangeMin, rangeMax) =>
        RangeBasedShufflePartitioning(
          dataTransferBatchSize,
          fromWorkerIds.flatMap(fromId =>
            toWorkerIds.map(toId => ChannelIdentity(fromId, toId, isControl = false))
          ),
          rangeAttributeNames,
          rangeMin,
          rangeMax
        )

      case SinglePartition() =>
        assert(toWorkerIds.size == 1)
        OneToOnePartitioning(
          dataTransferBatchSize,
          fromWorkerIds.map(fromWorkerId =>
            ChannelIdentity(fromWorkerId, toWorkerIds.head, isControl = false)
          )
        )

      case OneToOnePartition() =>
        OneToOnePartitioning(
          dataTransferBatchSize,
          fromWorkerIds.zip(toWorkerIds).map {
            case (fromWorkerId, toWorkerId) =>
              ChannelIdentity(fromWorkerId, toWorkerId, isControl = false)
          }
        )

      case BroadcastPartition() =>
        BroadcastPartitioning(
          dataTransferBatchSize,
          fromWorkerIds.zip(toWorkerIds).map {
            case (fromWorkerId, toWorkerId) =>
              ChannelIdentity(fromWorkerId, toWorkerId, isControl = false)
          }
        )

      case UnknownPartition() =>
        RoundRobinPartitioning(
          dataTransferBatchSize,
          fromWorkerIds.flatMap(from =>
            toWorkerIds.map(to => ChannelIdentity(from, to, isControl = false))
          )
        )

      case _ =>
        throw new UnsupportedOperationException()

    }
  }
}

case class LinkConfig(
    channelConfigs: List[ChannelConfig],
    partitioning: Partitioning
)
