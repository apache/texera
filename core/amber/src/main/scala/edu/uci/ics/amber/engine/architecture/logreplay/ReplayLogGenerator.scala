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

package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.core.virtualidentity.EmbeddedControlMessageIdentity

import scala.collection.mutable

object ReplayLogGenerator {
  def generate(
      logStorage: SequentialRecordStorage[ReplayLogRecord],
      logFileName: String,
      replayTo: EmbeddedControlMessageIdentity
  ): (mutable.Queue[ProcessingStep], mutable.Queue[WorkflowFIFOMessage]) = {
    val logs = logStorage.getReader(logFileName).mkRecordIterator()
    val steps = mutable.Queue[ProcessingStep]()
    val messages = mutable.Queue[WorkflowFIFOMessage]()
    logs.foreach {
      case s: ProcessingStep =>
        steps.enqueue(s)
      case MessageContent(message) =>
        messages.enqueue(message)
      case ReplayDestination(id) =>
        if (id == replayTo) {
          // we only need log record upto this point
          return (steps, messages)
        }
      case other =>
        throw new RuntimeException(s"cannot handle $other in the log")
    }
    (steps, messages)
  }
}
