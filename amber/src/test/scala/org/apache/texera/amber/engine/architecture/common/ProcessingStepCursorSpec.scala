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

package org.apache.texera.amber.engine.architecture.common

import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.scalatest.flatspec.AnyFlatSpec

class ProcessingStepCursorSpec extends AnyFlatSpec {

  private val channelA =
    ChannelIdentity(ActorVirtualIdentity("a"), ActorVirtualIdentity("b"), isControl = false)
  private val channelB =
    ChannelIdentity(ActorVirtualIdentity("a"), ActorVirtualIdentity("c"), isControl = true)

  "ProcessingStepCursor" should "start at INIT_STEP with no current channel" in {
    val cursor = new ProcessingStepCursor()
    assert(cursor.getStep == ProcessingStepCursor.INIT_STEP)
    assert(cursor.getStep == -1L)
    assert(cursor.getChannel == null)
  }

  "ProcessingStepCursor.stepIncrement" should "advance the step to 0 on the first call" in {
    val cursor = new ProcessingStepCursor()
    cursor.stepIncrement()
    assert(cursor.getStep == 0L)
  }

  it should "advance the step by exactly one each call" in {
    val cursor = new ProcessingStepCursor()
    (0 until 5).foreach(_ => cursor.stepIncrement())
    assert(cursor.getStep == 4L)
  }

  "ProcessingStepCursor.setCurrentChannel" should "store the latest channel" in {
    val cursor = new ProcessingStepCursor()
    cursor.setCurrentChannel(channelA)
    assert(cursor.getChannel == channelA)

    cursor.setCurrentChannel(channelB)
    assert(cursor.getChannel == channelB)
  }

  it should "leave the step counter unchanged" in {
    val cursor = new ProcessingStepCursor()
    cursor.stepIncrement()
    cursor.setCurrentChannel(channelA)
    assert(cursor.getStep == 0L)
  }
}
