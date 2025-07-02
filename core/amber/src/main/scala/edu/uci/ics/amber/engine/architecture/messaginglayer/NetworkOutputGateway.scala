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

package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.{
  DirectControlMessagePayload,
  DataPayload,
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/**
  * NetworkOutput for generating sequence number when sending payloads
  *
  * @param actorId ActorVirtualIdentity for the sender
  * @param handler actual sending logic
  */
class NetworkOutputGateway(
    val actorId: ActorVirtualIdentity,
    val handler: WorkflowFIFOMessage => Unit
) extends AmberLogging
    with Serializable {

  private val idToSequenceNums = new mutable.HashMap[ChannelIdentity, AtomicLong]()

  def addOutputChannel(channelId: ChannelIdentity): Unit = {
    if (!idToSequenceNums.contains(channelId)) {
      idToSequenceNums(channelId) = new AtomicLong()
    }
  }

  private def sendToInternal(
      to: ActorVirtualIdentity,
      useControlChannel: Boolean,
      payload: WorkflowFIFOMessagePayload
  ): Unit = {
    var receiverId = to
    if (to == SELF) {
      // selfID and VirtualIdentity.SELF should be one key
      receiverId = actorId
    }
    val outChannelId = ChannelIdentity(actorId, receiverId, useControlChannel)
    val seqNum = getSequenceNumber(outChannelId)
    handler(WorkflowFIFOMessage(outChannelId, seqNum, payload))
  }

  def sendTo(to: ActorVirtualIdentity, payload: DirectControlMessagePayload): Unit = {
    sendToInternal(to, useControlChannel = true, payload)
  }

  def sendTo(to: ActorVirtualIdentity, payload: DataPayload): Unit = {
    sendToInternal(to, useControlChannel = false, payload)
  }

  def sendTo(channelIdentity: ChannelIdentity, payload: WorkflowFIFOMessagePayload): Unit = {
    val destChannelId = if (channelIdentity.toWorkerId == SELF) {
      // selfID and VirtualIdentity.SELF should be one key
      ChannelIdentity(channelIdentity.fromWorkerId, actorId, channelIdentity.isControl)
    } else {
      channelIdentity
    }
    val seqNum = getSequenceNumber(destChannelId)
    handler(WorkflowFIFOMessage(destChannelId, seqNum, payload))
  }

  def getFIFOState: Map[ChannelIdentity, Long] = idToSequenceNums.map(x => (x._1, x._2.get())).toMap

  def getActiveChannels: Iterable[ChannelIdentity] = idToSequenceNums.keys

  def getSequenceNumber(channelId: ChannelIdentity): Long = {
    idToSequenceNums.getOrElseUpdate(channelId, new AtomicLong()).getAndIncrement()
  }

}
