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

import edu.uci.ics.amber.engine.architecture.common.ProcessingStepCursor.INIT_STEP
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.core.virtualidentity.{ChannelIdentity, EmbeddedControlMessageIdentity}

import scala.collection.mutable

class ReplayLoggerImpl extends ReplayLogger {

  private val tempLogs = mutable.ArrayBuffer[ReplayLogRecord]()

  private var currentChannelId: ChannelIdentity = _

  private var lastStep = INIT_STEP

  /**
    * Records the current processing step along with an associated message.
    * This method also monitors the channel information. If the new channel matches the last recorded channel
    * and there is no associated message for this step, the logging operation is bypassed.
    * Otherwise, it appends a ProcessingStep log record with the message content, provided the message exists.
    *
    * @param step    The current processing step.
    * @param channel The channel ID associated with the processing step.
    * @param message An optional message associated with the processing step.
    */
  override def logCurrentStepWithMessage(
      step: Long,
      channelId: ChannelIdentity,
      message: Option[WorkflowFIFOMessage]
  ): Unit = {
    if (currentChannelId == channelId && message.isEmpty) {
      return
    }
    currentChannelId = channelId
    lastStep = step
    tempLogs.append(ProcessingStep(channelId, step))
    if (message.isDefined) {
      tempLogs.append(MessageContent(message.get))
    }
  }

  /**
    * Called when the data processor attempts to output a message.
    * This method retrieves all accumulated log records and passes them to the writer thread for persistence.
    * It ensures the processing up to the current processing step is captured in the log records.
    *
    * @param step The current processing step.
    * @return An array of ReplayLogRecord containing all the log records up to the current step.
    */
  def drainCurrentLogRecords(step: Long): Array[ReplayLogRecord] = {
    if (lastStep != step) {
      lastStep = step
      tempLogs.append(ProcessingStep(currentChannelId, step))
    }
    val result = tempLogs.toArray
    tempLogs.clear()
    result
  }

  def markAsReplayDestination(id: EmbeddedControlMessageIdentity): Unit = {
    tempLogs.append(ReplayDestination(id))
  }
}
