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

import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity

import scala.collection.mutable

class InputManager(val actorId: ActorVirtualIdentity) extends AmberLogging {
  private var inputBatch: Array[Tuple] = _
  private var currentInputIdx: Int = -1
  var currentChannelId: ChannelIdentity = _

  private val ports: mutable.HashMap[PortIdentity, WorkerPort] = mutable.HashMap()

  def getAllPorts: Set[PortIdentity] = {
    this.ports.keys.toSet
  }

  def addPort(portId: PortIdentity, schema: Schema): Unit = {
    // each port can only be added and initialized once.
    if (this.ports.contains(portId)) {
      return
    }
    this.ports(portId) = WorkerPort(schema)
  }

  def getPort(portId: PortIdentity): WorkerPort = ports(portId)

  def isPortCompleted(portId: PortIdentity): Boolean = {
    // a port without channels is not completed.
    if (this.ports(portId).channels.isEmpty) {
      return false
    }
    this.ports(portId).channels.values.forall(completed => completed)
  }

  def hasUnfinishedInput: Boolean = inputBatch != null && currentInputIdx + 1 < inputBatch.length

  def getNextTuple: Tuple = {
    currentInputIdx += 1
    inputBatch(currentInputIdx)
  }

  def getCurrentTuple: Tuple = {
    if (inputBatch == null) {
      null
    } else if (inputBatch.isEmpty) {
      null // TODO: create input exhausted
    } else {
      inputBatch(currentInputIdx)
    }
  }

  def initBatch(channelId: ChannelIdentity, batch: Array[Tuple]): Unit = {
    currentChannelId = channelId
    inputBatch = batch
    currentInputIdx = -1
  }
}
