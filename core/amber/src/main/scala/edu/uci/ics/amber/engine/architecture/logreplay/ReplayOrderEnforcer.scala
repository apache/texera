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

import edu.uci.ics.amber.core.virtualidentity.ChannelIdentity

import scala.collection.mutable

class ReplayOrderEnforcer(
    logManager: ReplayLogManager,
    channelStepOrder: mutable.Queue[ProcessingStep],
    startStep: Long,
    private var onComplete: () => Unit
) extends OrderEnforcer {
  private var currentChannelId: ChannelIdentity = _

  private def triggerOnComplete(): Unit = {
    if (!isCompleted) {
      return
    }
    if (onComplete != null) {
      onComplete()
      onComplete = null // make sure the onComplete is called only once.
    }
  }

  // restore replay progress by dropping some of the entries
  while (channelStepOrder.nonEmpty && channelStepOrder.head.step <= startStep) {
    forwardNext()
  }

  var isCompleted: Boolean = channelStepOrder.isEmpty

  triggerOnComplete()

  private def forwardNext(): Unit = {
    if (channelStepOrder.nonEmpty) {
      val nextStep = channelStepOrder.dequeue()
      currentChannelId = nextStep.channelId
    }
  }

  def canProceed(channelId: ChannelIdentity): Boolean = {
    val step = logManager.getStep
    // release the next log record if the step matches
    // Note: To remove duplicate step orders caused by checkpoints
    // sending out a MainThreadDelegateMessage, we use a while loop.
    while (channelStepOrder.nonEmpty && channelStepOrder.head.step == step) {
      forwardNext()
    }
    // To terminate replay:
    // no next log record with step > current step, which means further processing is not logged.
    if (channelStepOrder.isEmpty) {
      isCompleted = true
      triggerOnComplete()
    }
    // only proceed if the current channel ID matches the channel ID of the log record
    currentChannelId == channelId
  }
}
