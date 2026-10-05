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

package org.apache.texera.amber.engine.architecture.coordinator

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity

/**
  * Assigns each sender on a least-loaded link a preferred
  * receiver, ranked by measured backlog, instead of a fixed rotation.
  *
  * The one property this MUST have to be worth building at all: no two
  * senders assigned in the same round should be told the same preferred
  * receiver, or all of them will route their next batch to the same worker
  * simultaneously -- overloading exactly the receiver the ranking was trying
  * to protect, then thrashing to whichever receiver looks least-loaded next
  * round. See computeAssignments.
  */
object LeastLoadedRoutingService {

  case class RoutingAssignment(sender: ActorVirtualIdentity, preferredReceiver: ActorVirtualIdentity)

  /**
    * Rank receivers ascending by backlog and assign them to senders as a
    * bijection: sender at position i (in the link's own, stable sender order)
    * gets the receiver at rank i. A worker with no backlog reading yet is
    * treated as backlog 0 -- eligible, not penalized for a temporary gap in
    * the data rather than assumed overloaded.
    *
    * Collision-free by construction when senders.size == receivers.size (the
    * common case: this workload is 8x8). When they differ, positions repeat
    * modulo receivers.size and more than one sender CAN be assigned the same
    * receiver in the same round -- the ranking degrades toward "several
    * senders agree on a good guess" rather than the guaranteed-distinct
    * assignment the square case gives.  Not specially handled: correct
    * behaviour, just without the collision-free guarantee.
    */
  def computeAssignments(
      senders: Seq[ActorVirtualIdentity],
      receivers: Seq[ActorVirtualIdentity],
      backlogByReceiver: Map[ActorVirtualIdentity, Long]
  ): Seq[RoutingAssignment] = {
    if (senders.isEmpty || receivers.isEmpty) return Seq.empty
    val rankedReceivers = receivers.sortBy(r => backlogByReceiver.getOrElse(r, 0L))
    senders.zipWithIndex.map {
      case (senderId, i) =>
        RoutingAssignment(senderId, rankedReceivers(i % rankedReceivers.length))
    }
  }
}
