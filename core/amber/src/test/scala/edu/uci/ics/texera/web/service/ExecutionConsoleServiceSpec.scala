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

package edu.uci.ics.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessage
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageType
import edu.uci.ics.amber.engine.common.executionruntimestate.ExecutionConsoleStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class ExecutionConsoleServiceSpec extends AnyFlatSpec with Matchers {

  // Constants for testing
  val standardBufferSize: Int = 100
  val smallBufferSize: Int = 2
  val messageDisplayLength: Int = 100

  "processConsoleMessage" should "truncate message title when it exceeds display length" in {
    // Create a long message title that exceeds display length
    val longTitle = "a" * (messageDisplayLength + 10)
    val expectedTruncatedTitle = "a" * (messageDisplayLength - 3) + "..."

    // Create a console message with a long title
    val consoleMessage = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      longTitle,
      "message content"
    )

    // Call the method under test
    val processedMessage =
      ConsoleMessageProcessor.processConsoleMessage(consoleMessage, messageDisplayLength)

    // Verify the title was truncated
    processedMessage.title shouldBe expectedTruncatedTitle
  }

  it should "not truncate message title when it does not exceed display length" in {
    // Create a short message title that doesn't exceed display length
    val shortTitle = "Short Title"

    // Create a console message with a short title
    val consoleMessage = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      shortTitle,
      "message content"
    )

    // Call the method under test
    val processedMessage =
      ConsoleMessageProcessor.processConsoleMessage(consoleMessage, messageDisplayLength)

    // Verify the title was not truncated
    processedMessage.title shouldBe shortTitle
  }

  "addMessageToOperatorConsole" should "add message to buffer when buffer is not full" in {
    // Create a test console store
    val consoleStore = new ExecutionConsoleStore()
    val opId = "op1"

    // Create console messages
    val message1 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 1",
      "content 1"
    )

    val message2 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 2",
      "content 2"
    )

    // Add first message
    val storeWithMessage1 =
      ConsoleMessageProcessor.addMessageToOperatorConsole(
        consoleStore,
        opId,
        message1,
        standardBufferSize
      )

    // Add second message
    val storeWithMessage2 = ConsoleMessageProcessor.addMessageToOperatorConsole(
      storeWithMessage1,
      opId,
      message2,
      standardBufferSize
    )

    // Verify both messages are in the buffer
    val opInfo = storeWithMessage2.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 2
    opInfo.consoleMessages.head.title shouldBe "Message 1"
    opInfo.consoleMessages(1).title shouldBe "Message 2"
  }

  it should "remove oldest message when buffer is full" in {
    // Create a test console store
    val consoleStore = new ExecutionConsoleStore()
    val opId = "op1"

    // Create console messages
    val message1 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 1",
      "content 1"
    )

    val message2 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 2",
      "content 2"
    )

    val message3 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 3",
      "content 3"
    )

    // Fill the buffer
    val storeWithMessage1 =
      ConsoleMessageProcessor.addMessageToOperatorConsole(
        consoleStore,
        opId,
        message1,
        smallBufferSize
      )
    val storeWithMessage2 =
      ConsoleMessageProcessor.addMessageToOperatorConsole(
        storeWithMessage1,
        opId,
        message2,
        smallBufferSize
      )

    // Add one more message which should remove the oldest
    val storeWithMessage3 =
      ConsoleMessageProcessor.addMessageToOperatorConsole(
        storeWithMessage2,
        opId,
        message3,
        smallBufferSize
      )

    // Verify the first message was removed and only the second and third remain
    val opInfo = storeWithMessage3.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 2
    opInfo.consoleMessages.head.title shouldBe "Message 2"
    opInfo.consoleMessages(1).title shouldBe "Message 3"
  }

  "the complete message processing flow" should "handle messages correctly" in {
    // Create a test console store
    val consoleStore = new ExecutionConsoleStore()
    val opId = "op1"

    // Create a message with a title that needs truncation
    val longTitle = "a" * (messageDisplayLength + 10)
    val consoleMessage = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      longTitle,
      "message content"
    )

    // Process the message first
    val processedMessage =
      ConsoleMessageProcessor.processConsoleMessage(consoleMessage, messageDisplayLength)

    // Then update the store
    val updatedStore = ConsoleMessageProcessor.addMessageToOperatorConsole(
      consoleStore,
      opId,
      processedMessage,
      standardBufferSize
    )

    // Verify correct processing
    val opInfo = updatedStore.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 1

    // Check that title was truncated
    val expectedTruncatedTitle = "a" * (messageDisplayLength - 3) + "..."
    opInfo.consoleMessages.head.title shouldBe expectedTruncatedTitle
  }
}
