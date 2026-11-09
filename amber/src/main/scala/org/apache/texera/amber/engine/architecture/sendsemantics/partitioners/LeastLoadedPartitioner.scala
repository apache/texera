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

import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.LeastLoadedPartitioning

/** Sends every tuple to one receiver -- the one the coordinator currently ranks
  * least backlogged -- rather than dealing tuples out across all of them.
  *
  * Round robin advances a tuple at a time, so each receiver's output buffer
  * fills at 1/receivers the rate and a link holds senders x receivers partly
  * filled buffers before any of them reaches batchSize. Concentrating on one
  * receiver fills one buffer at the full rate, so the first batch ships after
  * batchSize tuples regardless of how wide the link is.
  *
  * The index is replaced wholesale by UpdateRoutingPreference on each
  * statistics poll. Buffers left partly filled when the preference moves are
  * not flushed early -- that would ship a short batch and break the configured
  * batch size, which is the thing this design exists to preserve. They are
  * drained when that receiver is preferred again, or by the end-of-stream flush.
  */
case class LeastLoadedPartitioner(
    partitioning: LeastLoadedPartitioning,
    actorId: ActorVirtualIdentity
) extends Partitioner {

  private val receivers: Seq[ActorVirtualIdentity] = partitioning.channels.map(_.toWorkerId).distinct

  // Written by the control thread handling UpdateRoutingPreference, read by the
  // data-processing thread. @volatile rather than a lock: a stale read costs one
  // batch sent to the previous receiver, which the next poll corrects.
  //
  // Seeded by this sender's position among the link's senders rather than 0, so
  // the batches that fill before the first ranking arrives are spread instead of
  // landing on receiver 0 together. Derived from the partitioning's own channel
  // list, which every sender holds identically, so no two senders disagree.
  @volatile private var preferredIndex: Int = {
    val senders = partitioning.channels.map(_.fromWorkerId).distinct
    val position = senders.indexOf(actorId)
    if (position >= 0 && receivers.nonEmpty) position % receivers.length else 0
  }

  /** Ignores an out-of-range index rather than failing: the coordinator derives
    * it from its own copy of the channel list, and a mismatch during
    * reconfiguration should degrade to the last good target, not kill the worker.
    */
  def setPreferredReceiverIndex(index: Int): Unit = {
    if (index >= 0 && index < receivers.length) {
      preferredIndex = index
    }
  }

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = Iterator(preferredIndex)

  override def allReceivers: Seq[ActorVirtualIdentity] = receivers
}
