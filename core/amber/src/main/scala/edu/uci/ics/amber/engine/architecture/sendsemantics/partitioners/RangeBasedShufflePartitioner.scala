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

import edu.uci.ics.amber.core.tuple.{AttributeType, Tuple}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.RangeBasedShufflePartitioning
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

case class RangeBasedShufflePartitioner(partitioning: RangeBasedShufflePartitioning)
    extends Partitioner {

  private val receivers = partitioning.channels.map(_.toWorkerId).distinct
  private val keysPerReceiver =
    ((partitioning.rangeMax - partitioning.rangeMin) / receivers.length) + 1

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = {
    // Do range partitioning only on the first attribute in `rangeAttributeNames`.
    val attribute = tuple.getSchema.getAttribute(partitioning.rangeAttributeNames.head)
    var fieldVal: Long = -1
    attribute.getType match {
      case AttributeType.LONG =>
        fieldVal = tuple.getField[Long](attribute)
      case AttributeType.INTEGER =>
        fieldVal = tuple.getField[Int](attribute)
      case AttributeType.DOUBLE =>
        fieldVal = tuple.getField[Double](attribute).toLong
      case _ =>
        throw new RuntimeException(s"unsupported attribute type: ${attribute.getType}")
    }

    if (fieldVal < partitioning.rangeMin) {
      return Iterator(0)
    }
    if (fieldVal > partitioning.rangeMax) {
      return Iterator(receivers.length - 1)
    }
    Iterator(((fieldVal - partitioning.rangeMin) / keysPerReceiver).toInt)
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = receivers

}
