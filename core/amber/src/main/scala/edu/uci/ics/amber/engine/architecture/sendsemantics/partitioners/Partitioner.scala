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

import edu.uci.ics.amber.config.ApplicationConfig
import edu.uci.ics.amber.core.state.State
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, StateFrame}
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity
import scala.collection.mutable.ArrayBuffer

trait Partitioner extends Serializable {
  def getBucketIndex(tuple: Tuple): Iterator[Int]

  def allReceivers: Seq[ActorVirtualIdentity]
}

class NetworkOutputBuffer(
    val to: ActorVirtualIdentity,
    val dataOutputPort: NetworkOutputGateway,
    val batchSize: Int = ApplicationConfig.defaultDataTransferBatchSize
) {

  var buffer = new ArrayBuffer[Tuple]()

  def addTuple(tuple: Tuple): Unit = {
    buffer.append(tuple)
    if (buffer.size >= batchSize) {
      flush()
    }
  }

  def sendState(state: State): Unit = {
    flush()
    dataOutputPort.sendTo(to, StateFrame(state))
    flush()
  }

  def flush(): Unit = {
    if (buffer.nonEmpty) {
      dataOutputPort.sendTo(to, DataFrame(buffer.toArray))
      buffer = new ArrayBuffer[Tuple]()
    }
  }

}
