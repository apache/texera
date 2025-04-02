package edu.uci.ics.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessage
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageType
import edu.uci.ics.amber.engine.common.executionruntimestate.ExecutionConsoleStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class ExecutionConsoleServiceSpec extends AnyFlatSpec with Matchers {

  // Create test services with different configurations
  // Instead of mocking, we pass null for dependencies we don't use in our tests
  class TestExecutionConsoleService
      extends ExecutionConsoleService(
        null,
        null,
        null,
        null
      ) {
    override val bufferSize: Int = 100
    override val consoleMessageDisplayLength: Int = 100
  }

  class SmallBufferExecutionConsoleService
      extends ExecutionConsoleService(
        null,
        null,
        null,
        null
      ) {
    override val bufferSize: Int = 2
    override val consoleMessageDisplayLength: Int = 100
  }

  "processConsoleMessage" should "truncate message title when it exceeds display length" in {
    val service = new TestExecutionConsoleService

    // Create a long message title that exceeds display length
    val longTitle = "a" * (service.consoleMessageDisplayLength + 10)
    val expectedTruncatedTitle = "a" * (service.consoleMessageDisplayLength - 3) + "..."

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
    val processedMessage = service.processConsoleMessage(consoleMessage)

    // Verify the title was truncated
    processedMessage.title shouldBe expectedTruncatedTitle
  }

  it should "not truncate message title when it does not exceed display length" in {
    val service = new TestExecutionConsoleService

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
    val processedMessage = service.processConsoleMessage(consoleMessage)

    // Verify the title was not truncated
    processedMessage.title shouldBe shortTitle
  }

  "updateConsoleStore" should "add message to buffer when buffer is not full" in {
    val service = new TestExecutionConsoleService

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
    val storeWithMessage1 = service.updateConsoleStore(consoleStore, opId, message1)

    // Add second message
    val storeWithMessage2 = service.updateConsoleStore(storeWithMessage1, opId, message2)

    // Verify both messages are in the buffer
    val opInfo = storeWithMessage2.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 2
    opInfo.consoleMessages.head.title shouldBe "Message 1"
    opInfo.consoleMessages(1).title shouldBe "Message 2"
  }

  it should "remove oldest message when buffer is full" in {
    val service = new SmallBufferExecutionConsoleService

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
    val storeWithMessage1 = service.updateConsoleStore(consoleStore, opId, message1)
    val storeWithMessage2 = service.updateConsoleStore(storeWithMessage1, opId, message2)

    // Add one more message which should remove the oldest
    val storeWithMessage3 = service.updateConsoleStore(storeWithMessage2, opId, message3)

    // Verify the first message was removed and only the second and third remain
    val opInfo = storeWithMessage3.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 2
    opInfo.consoleMessages.head.title shouldBe "Message 2"
    opInfo.consoleMessages(1).title shouldBe "Message 3"
  }

  "the complete message processing flow" should "handle messages correctly" in {
    val service = new TestExecutionConsoleService

    // Create a test console store
    val consoleStore = new ExecutionConsoleStore()
    val opId = "op1"

    // Create a message with a title that needs truncation
    val longTitle = "a" * (service.consoleMessageDisplayLength + 10)
    val consoleMessage = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      longTitle,
      "message content"
    )

    // Process the message first
    val processedMessage = service.processConsoleMessage(consoleMessage)

    // Then update the store
    val updatedStore = service.updateConsoleStore(consoleStore, opId, processedMessage)

    // Verify correct processing
    val opInfo = updatedStore.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 1

    // Check that title was truncated
    val expectedTruncatedTitle = "a" * (service.consoleMessageDisplayLength - 3) + "..."
    opInfo.consoleMessages.head.title shouldBe expectedTruncatedTitle
  }
}
