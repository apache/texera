package edu.uci.ics.amber.operator.source.scan.text

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import edu.uci.ics.amber.operator.source.scan.FileScanSourceOpExec
import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.util.{Failure, Try}

class FileScanSourceOpExecSpec extends AnyFlatSpec with Matchers {

  // Create a mock subclass to test just the readToByteBuffers method
  private class TestableFileScanSourceOpExec
      extends FileScanSourceOpExec("""{"someRequiredField":"dummyValue"}""") {
    def exposedReadToByteBuffers(input: InputStream): List[ByteBuffer] = {
      readToByteBuffers(input)
    }
  }

  private val testableExec = new TestableFileScanSourceOpExec()

  "readToByteBuffers" should "handle empty input stream" in {
    val emptyStream = new ByteArrayInputStream(Array.emptyByteArray)
    val result = testableExec.exposedReadToByteBuffers(emptyStream)

    result shouldBe empty
  }

  it should "read all data from a small input stream" in {
    val testData = "Hello, World!".getBytes(StandardCharsets.UTF_8)
    val inputStream = new ByteArrayInputStream(testData)

    val result = testableExec.exposedReadToByteBuffers(inputStream)

    result should have size 1
    val buffer = result.head
    buffer.capacity() shouldBe testData.length

    val resultBytes = new Array[Byte](buffer.remaining())
    buffer.get(resultBytes)
    resultBytes shouldEqual testData
  }

  it should "split large data into multiple buffers" in {
    // Create a mock InputStream that simulates a large file
    // For testing, we'll use a much smaller buffer size to avoid creating huge test data
    val mockLargeData = new InputStream {
      private val chunkSize = 100 // Small for testing
      private val totalChunks = 3
      private var currentChunk = 0
      private var position = 0

      override def read(): Int = {
        if (currentChunk >= totalChunks) return -1

        if (position < chunkSize) {
          position += 1
          return 65 + (currentChunk % 26) // Return 'A', 'B', 'C', etc.
        } else {
          position = 1
          currentChunk += 1
          if (currentChunk >= totalChunks) return -1
          return 65 + (currentChunk % 26)
        }
      }

      override def read(b: Array[Byte], off: Int, len: Int): Int = {
        if (currentChunk >= totalChunks) return -1

        val availableInChunk = chunkSize - position
        if (availableInChunk <= 0) {
          position = 0
          currentChunk += 1
          if (currentChunk >= totalChunks) return -1
          return read(b, off, len)
        }

        val bytesToRead = Math.min(len, availableInChunk)
        for (i <- 0 until bytesToRead) {
          b(off + i) = (65 + (currentChunk % 26)).toByte // Fill with 'A', 'B', 'C', etc.
        }

        position += bytesToRead
        bytesToRead
      }
    }

    val result = testableExec.exposedReadToByteBuffers(mockLargeData)

    result should have size 3

    // Verify each buffer has the expected content
    for (i <- 0 until 3) {
      val buffer = result(i)
      buffer.remaining() shouldBe 100

      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)

      // All bytes should be the same letter ('A', 'B', or 'C')
      bytes.foreach(_ shouldBe (65 + i).toByte)
    }
  }

  it should "properly close the input stream" in {
    var streamClosed = false
    val mockStream = new ByteArrayInputStream("test".getBytes) {
      override def close(): Unit = {
        streamClosed = true
        super.close()
      }
    }

    testableExec.exposedReadToByteBuffers(mockStream)
    streamClosed shouldBe true
  }

  it should "handle exceptions properly" in {
    val failingStream = new InputStream {
      override def read(): Int = throw new IOException("Simulated failure")
      override def read(b: Array[Byte], off: Int, len: Int): Int =
        throw new IOException("Simulated failure")
    }

    val result = Try(testableExec.exposedReadToByteBuffers(failingStream))
    result shouldBe a[Failure[_]]
    result.failed.get shouldBe an[IOException]
  }
}
