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
import org.scalatest.flatspec.AnyFlatSpec

class LeastLoadedRoutingServiceSpec extends AnyFlatSpec {

  private def workers(prefix: String, n: Int): Seq[ActorVirtualIdentity] =
    (0 until n).map(i => ActorVirtualIdentity(s"$prefix-$i"))

  "computeAssignments" should "give no two senders the same receiver when counts match" in {
    val senders = workers("sender", 8)
    val receivers = workers("receiver", 8)
    // Deliberately lopsided, including ties, so ordering cannot be accidental.
    val backlog = receivers.zip(Seq(500L, 1000L, 200L, 2000L, 0L, 200L, 50L, 50L)).toMap

    val assignments = LeastLoadedRoutingService.computeAssignments(senders, receivers, backlog)

    assert(assignments.size == 8)
    // The property the design rests on: without it every sender ships its next
    // batch to the same worker at once, burying exactly the receiver the
    // ranking meant to favour.
    assert(assignments.map(_.preferredReceiver).distinct.size == 8)
    assert(assignments.map(_.sender) == senders)
  }

  it should "hand the least backlogged receiver to the first sender" in {
    val senders = workers("sender", 4)
    val receivers = workers("receiver", 4)
    val backlog = receivers.zip(Seq(500L, 1000L, 200L, 2000L)).toMap

    val assignments = LeastLoadedRoutingService.computeAssignments(senders, receivers, backlog)

    assert(assignments.head.preferredReceiver == receivers(2)) // backlog 200
    assert(assignments.last.preferredReceiver == receivers(3)) // backlog 2000
    // Senders are served in ascending backlog order overall.
    val assignedBacklogs = assignments.map(a => backlog(a.preferredReceiver))
    assert(assignedBacklogs == assignedBacklogs.sorted)
  }

  it should "treat a receiver with no reading as idle rather than overloaded" in {
    val senders = workers("sender", 2)
    val receivers = workers("receiver", 2)
    // Only one receiver reported this round; the other should still be eligible,
    // not penalised for a gap in the data.
    val backlog = Map(receivers.head -> 900L)

    val assignments = LeastLoadedRoutingService.computeAssignments(senders, receivers, backlog)

    assert(assignments.head.preferredReceiver == receivers(1))
  }

  it should "return nothing when either side of the link is empty" in {
    val someWorkers = workers("worker", 3)
    assert(LeastLoadedRoutingService.computeAssignments(Seq.empty, someWorkers, Map.empty).isEmpty)
    assert(LeastLoadedRoutingService.computeAssignments(someWorkers, Seq.empty, Map.empty).isEmpty)
  }

  it should "still assign every sender when senders outnumber receivers" in {
    val senders = workers("sender", 5)
    val receivers = workers("receiver", 2)
    val backlog = receivers.zip(Seq(10L, 5L)).toMap

    val assignments = LeastLoadedRoutingService.computeAssignments(senders, receivers, backlog)

    // Collisions are unavoidable here and are not treated as an error: the
    // guarantee degrades to "several senders agree on a good guess".
    assert(assignments.size == 5)
    assert(assignments.map(_.preferredReceiver).forall(receivers.contains))
    assert(assignments.head.preferredReceiver == receivers(1)) // backlog 5
  }
}
