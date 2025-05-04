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

package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.HashBasedShufflePartitioning
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

case class HashBasedShufflePartitioner(partitioning: HashBasedShufflePartitioning)
    extends Partitioner {

  private val receivers = partitioning.channels.map(_.toWorkerId).distinct

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = {
    val numBuckets = receivers.length
    val partialTuple =
      if (partitioning.hashAttributeNames.isEmpty) tuple
      else tuple.getPartialTuple(partitioning.hashAttributeNames.toList)
    val index = Math.floorMod(partialTuple.hashCode(), numBuckets)
    Iterator(index)
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = receivers
}
