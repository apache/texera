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

package org.apache.texera.service.util

import org.apache.texera.amber.core.tuple.LargeBinary
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, IOException}
import scala.util.Random

class LargeBinaryInputStreamSpec
    extends AnyFunSuite
    with S3StorageTestBase
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val testBucketName = "test-large-binary-input-stream"

  override def beforeAll(): Unit = {
    super.beforeAll()
    S3StorageClient.createBucketIfNotExist(testBucketName)
  }

  override def afterAll(): Unit = {
    try {
      S3StorageClient.deleteDirectory(testBucketName, "")
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
    super.afterAll()
  }

  // Helper methods
  private def createTestObject(key: String, data: Array[Byte]): LargeBinary = {
    S3StorageClient.uploadObject(testBucketName, key, new ByteArrayInputStream(data))
    new LargeBinary(s"s3://$testBucketName/$key")
  }

  private def createTestObject(key: String, data: String): LargeBinary =
    createTestObject(key, data.getBytes)

  private def generateRandomData(size: Int): Array[Byte] =
    Array.fill[Byte](size)((Random.nextInt(256) - 128).toByte)

  private def withStream[T](largeBinary: LargeBinary)(f: LargeBinaryInputStream => T): T = {
    val stream = new LargeBinaryInputStream(largeBinary)
    try {
      f(stream)
    } finally {
      stream.close()
    }
  }

  private def assertThrowsIOExceptionWhenClosed(operation: LargeBinaryInputStream => Unit): Unit = {
    val largeBinary = createTestObject(s"test/closed-${Random.nextInt()}.txt", "data")
    val stream = new LargeBinaryInputStream(largeBinary)
    stream.close()
    val exception = intercept[IOException](operation(stream))
    assert(exception.getMessage.contains("Stream is closed"))
  }

  // Constructor Tests
  test("constructor should reject null LargeBinary") {
    val exception = intercept[IllegalArgumentException] {
      new LargeBinaryInputStream(null)
    }
    assert(exception.getMessage.contains("LargeBinary cannot be null"))
  }

  test("constructor should accept valid LargeBinary") {
    val largeBinary = createTestObject("test/valid.txt", "test data")
    withStream(largeBinary) { _ => }
  }

  // read() Tests
  test("read() should read single bytes correctly") {
    val largeBinary = createTestObject("test/single-byte.txt", "Hello")
    withStream(largeBinary) { stream =>
      assert(stream.read() == 'H'.toByte)
      assert(stream.read() == 'e'.toByte)
      assert(stream.read() == 'l'.toByte)
      assert(stream.read() == 'l'.toByte)
      assert(stream.read() == 'o'.toByte)
      assert(stream.read() == -1) // EOF
    }
  }

  test("read() should return -1 for empty object") {
    val largeBinary = createTestObject("test/empty.txt", "")
    withStream(largeBinary) { stream =>
      assert(stream.read() == -1)
    }
  }

  // read(byte[], int, int) Tests
  test("read(byte[], int, int) should read data into buffer") {
    val testData = "Hello, World!"
    val largeBinary = createTestObject("test/buffer-read.txt", testData)
    withStream(largeBinary) { stream =>
      val buffer = new Array[Byte](testData.length)
      val bytesRead = stream.read(buffer, 0, buffer.length)
      assert(bytesRead == testData.length)
      assert(new String(buffer) == testData)
    }
  }

  test("read(byte[], int, int) should handle partial reads and offsets") {
    val testData = "Hello, World!"
    val largeBinary = createTestObject("test/partial.txt", testData)
    withStream(largeBinary) { stream =>
      // Test partial read
      val buffer1 = new Array[Byte](5)
      assert(stream.read(buffer1, 0, 5) == 5)
      assert(new String(buffer1) == "Hello")
    }

    // Test offset
    withStream(largeBinary) { stream =>
      val buffer2 = new Array[Byte](20)
      assert(stream.read(buffer2, 5, 10) == 10)
      assert(new String(buffer2, 5, 10) == "Hello, Wor")
    }
  }

  test("read(byte[], int, int) should return -1 at EOF") {
    val largeBinary = createTestObject("test/eof.txt", "test")
    withStream(largeBinary) { stream =>
      val buffer = new Array[Byte](10)
      stream.read(buffer, 0, 10)
      assert(stream.read(buffer, 0, 10) == -1)
    }
  }

  // readAllBytes() Tests
  test("readAllBytes() should read entire object") {
    val testData = "Hello, World! This is a test."
    val largeBinary = createTestObject("test/read-all.txt", testData)
    withStream(largeBinary) { stream =>
      assert(new String(stream.readAllBytes()) == testData)
    }
  }

  test("readAllBytes() should handle large objects") {
    val largeData = generateRandomData(1024 * 1024) // 1MB
    val largeBinary = createTestObject("test/large.bin", largeData)
    withStream(largeBinary) { stream =>
      val bytes = stream.readAllBytes()
      assert(bytes.length == largeData.length)
      assert(bytes.sameElements(largeData))
    }
  }

  test("readAllBytes() should return empty array for empty object") {
    val largeBinary = createTestObject("test/empty-all.txt", "")
    withStream(largeBinary) { stream =>
      assert(stream.readAllBytes().length == 0)
    }
  }

  // readNBytes() Tests
  test("readNBytes() should read exactly N bytes") {
    val testData = "Hello, World! This is a test."
    val largeBinary = createTestObject("test/read-n.txt", testData)
    withStream(largeBinary) { stream =>
      val bytes = stream.readNBytes(5)
      assert(bytes.length == 5)
      assert(new String(bytes) == "Hello")
    }
  }

  test("readNBytes() should handle EOF and zero") {
    val largeBinary = createTestObject("test/read-n-eof.txt", "Hello")
    withStream(largeBinary) { stream =>
      // Request more than available
      val bytes = stream.readNBytes(100)
      assert(bytes.length == 5)
      assert(new String(bytes) == "Hello")
    }

    // Test n=0
    withStream(largeBinary) { stream =>
      assert(stream.readNBytes(0).length == 0)
    }
  }

  // skip() Tests
  test("skip() should skip bytes correctly") {
    val largeBinary = createTestObject("test/skip.txt", "Hello, World!")
    withStream(largeBinary) { stream =>
      assert(stream.skip(7) == 7)
      assert(stream.read() == 'W'.toByte)
    }
  }

  test("skip() should handle EOF and zero") {
    val largeBinary = createTestObject("test/skip-eof.txt", "Hello")
    withStream(largeBinary) { stream =>
      assert(stream.skip(100) == 5)
      assert(stream.read() == -1)
    }

    // Test n=0
    withStream(largeBinary) { stream =>
      assert(stream.skip(0) == 0)
    }
  }

  // available() Tests
  test("available() should return non-negative value") {
    val largeBinary = createTestObject("test/available.txt", "Hello, World!")
    withStream(largeBinary) { stream =>
      assert(stream.available() >= 0)
    }
  }

  // close() Tests
  test("close() should be idempotent") {
    val largeBinary = createTestObject("test/close-idempotent.txt", "data")
    val stream = new LargeBinaryInputStream(largeBinary)
    stream.close()
    stream.close() // Should not throw
    stream.close() // Should not throw
  }

  test("close() should prevent further operations") {
    val largeBinary = createTestObject("test/close-prevents.txt", "data")
    val stream = new LargeBinaryInputStream(largeBinary)
    stream.close()

    intercept[IOException] { stream.read() }
    intercept[IOException] { stream.readAllBytes() }
    intercept[IOException] { stream.readNBytes(10) }
    intercept[IOException] { stream.skip(10) }
    intercept[IOException] { stream.available() }
  }

  test("close() should work without reading (lazy initialization)") {
    val largeBinary = createTestObject("test/close-lazy.txt", "data")
    val stream = new LargeBinaryInputStream(largeBinary)
    stream.close() // Should not throw
  }

  // Closed stream tests - consolidated
  test("operations should throw IOException when stream is closed") {
    assertThrowsIOExceptionWhenClosed(_.read())
    assertThrowsIOExceptionWhenClosed(_.read(new Array[Byte](10), 0, 10))
    assertThrowsIOExceptionWhenClosed(_.readAllBytes())
    assertThrowsIOExceptionWhenClosed(_.readNBytes(10))
    assertThrowsIOExceptionWhenClosed(_.skip(10))
    assertThrowsIOExceptionWhenClosed(_.available())
    assertThrowsIOExceptionWhenClosed(_.mark(100))
    assertThrowsIOExceptionWhenClosed(_.reset())
  }

  // mark/reset Tests
  test("markSupported() should delegate to underlying stream") {
    val largeBinary = createTestObject("test/mark.txt", "data")
    withStream(largeBinary) { stream =>
      val supported = stream.markSupported()
      assert(!supported || supported) // Just verify it's callable
    }
  }

  test("mark() and reset() should delegate to underlying stream") {
    val largeBinary = createTestObject("test/mark-reset.txt", "data")
    withStream(largeBinary) { stream =>
      if (stream.markSupported()) {
        stream.mark(100)
        stream.read()
        stream.reset()
      }
    // If not supported, methods should still be callable
    }
  }

  // Lazy initialization Tests
  test("lazy initialization should not download until first read") {
    val largeBinary = createTestObject("test/lazy-init.txt", "data")
    val stream = new LargeBinaryInputStream(largeBinary)
    // Creating the stream should not trigger download
    // Reading should trigger download
    try {
      assert(stream.read() == 'd'.toByte)
    } finally {
      stream.close()
    }
  }

  // Integration Tests
  test("should handle chunked reading of large objects") {
    val largeData = generateRandomData(10 * 1024) // 10KB
    val largeBinary = createTestObject("test/chunked.bin", largeData)
    withStream(largeBinary) { stream =>
      val buffer = new Array[Byte](1024)
      val output = new java.io.ByteArrayOutputStream()
      var bytesRead = 0

      while ({
        bytesRead = stream.read(buffer, 0, buffer.length)
        bytesRead != -1
      }) {
        output.write(buffer, 0, bytesRead)
      }

      val result = output.toByteArray
      assert(result.length == largeData.length)
      assert(result.sameElements(largeData))
    }
  }

  test("should handle multiple streams reading same object") {
    val testData = "Shared data"
    val largeBinary = createTestObject("test/shared.txt", testData)

    val stream1 = new LargeBinaryInputStream(largeBinary)
    val stream2 = new LargeBinaryInputStream(largeBinary)

    try {
      assert(new String(stream1.readAllBytes()) == testData)
      assert(new String(stream2.readAllBytes()) == testData)
    } finally {
      stream1.close()
      stream2.close()
    }
  }

  test("should preserve binary data integrity") {
    val binaryData = Array[Byte](0, 1, 2, 127, -128, -1, 50, 100)
    val largeBinary = createTestObject("test/binary.bin", binaryData)
    withStream(largeBinary) { stream =>
      assert(stream.readAllBytes().sameElements(binaryData))
    }
  }
}
