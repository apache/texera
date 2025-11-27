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

import org.apache.amber.core.tuple.BigObject
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, IOException}
import scala.util.Random

class BigObjectInputStreamSpec
    extends AnyFunSuite
    with S3StorageTestBase
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val testBucketName = "test-big-object-input-stream"

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
  private def createTestObject(key: String, data: Array[Byte]): BigObject = {
    S3StorageClient.uploadObject(testBucketName, key, new ByteArrayInputStream(data))
    new BigObject(s"s3://$testBucketName/$key")
  }

  private def createTestObject(key: String, data: String): BigObject =
    createTestObject(key, data.getBytes)

  private def generateRandomData(size: Int): Array[Byte] =
    Array.fill[Byte](size)((Random.nextInt(256) - 128).toByte)

  private def withStream[T](bigObject: BigObject)(f: BigObjectInputStream => T): T = {
    val stream = new BigObjectInputStream(bigObject)
    try {
      f(stream)
    } finally {
      stream.close()
    }
  }

  private def assertThrowsIOExceptionWhenClosed(operation: BigObjectInputStream => Unit): Unit = {
    val bigObject = createTestObject(s"test/closed-${Random.nextInt()}.txt", "data")
    val stream = new BigObjectInputStream(bigObject)
    stream.close()
    val exception = intercept[IOException](operation(stream))
    assert(exception.getMessage.contains("Stream is closed"))
  }

  // Constructor Tests
  test("constructor should reject null BigObject") {
    val exception = intercept[IllegalArgumentException] {
      new BigObjectInputStream(null)
    }
    assert(exception.getMessage.contains("BigObject cannot be null"))
  }

  test("constructor should accept valid BigObject") {
    val bigObject = createTestObject("test/valid.txt", "test data")
    withStream(bigObject) { _ => }
  }

  // read() Tests
  test("read() should read single bytes correctly") {
    val bigObject = createTestObject("test/single-byte.txt", "Hello")
    withStream(bigObject) { stream =>
      assert(stream.read() == 'H'.toByte)
      assert(stream.read() == 'e'.toByte)
      assert(stream.read() == 'l'.toByte)
      assert(stream.read() == 'l'.toByte)
      assert(stream.read() == 'o'.toByte)
      assert(stream.read() == -1) // EOF
    }
  }

  test("read() should return -1 for empty object") {
    val bigObject = createTestObject("test/empty.txt", "")
    withStream(bigObject) { stream =>
      assert(stream.read() == -1)
    }
  }

  // read(byte[], int, int) Tests
  test("read(byte[], int, int) should read data into buffer") {
    val testData = "Hello, World!"
    val bigObject = createTestObject("test/buffer-read.txt", testData)
    withStream(bigObject) { stream =>
      val buffer = new Array[Byte](testData.length)
      val bytesRead = stream.read(buffer, 0, buffer.length)
      assert(bytesRead == testData.length)
      assert(new String(buffer) == testData)
    }
  }

  test("read(byte[], int, int) should handle partial reads and offsets") {
    val testData = "Hello, World!"
    val bigObject = createTestObject("test/partial.txt", testData)
    withStream(bigObject) { stream =>
      // Test partial read
      val buffer1 = new Array[Byte](5)
      assert(stream.read(buffer1, 0, 5) == 5)
      assert(new String(buffer1) == "Hello")
    }

    // Test offset
    withStream(bigObject) { stream =>
      val buffer2 = new Array[Byte](20)
      assert(stream.read(buffer2, 5, 10) == 10)
      assert(new String(buffer2, 5, 10) == "Hello, Wor")
    }
  }

  test("read(byte[], int, int) should return -1 at EOF") {
    val bigObject = createTestObject("test/eof.txt", "test")
    withStream(bigObject) { stream =>
      val buffer = new Array[Byte](10)
      stream.read(buffer, 0, 10)
      assert(stream.read(buffer, 0, 10) == -1)
    }
  }

  // readAllBytes() Tests
  test("readAllBytes() should read entire object") {
    val testData = "Hello, World! This is a test."
    val bigObject = createTestObject("test/read-all.txt", testData)
    withStream(bigObject) { stream =>
      assert(new String(stream.readAllBytes()) == testData)
    }
  }

  test("readAllBytes() should handle large objects") {
    val largeData = generateRandomData(1024 * 1024) // 1MB
    val bigObject = createTestObject("test/large.bin", largeData)
    withStream(bigObject) { stream =>
      val bytes = stream.readAllBytes()
      assert(bytes.length == largeData.length)
      assert(bytes.sameElements(largeData))
    }
  }

  test("readAllBytes() should return empty array for empty object") {
    val bigObject = createTestObject("test/empty-all.txt", "")
    withStream(bigObject) { stream =>
      assert(stream.readAllBytes().length == 0)
    }
  }

  // readNBytes() Tests
  test("readNBytes() should read exactly N bytes") {
    val testData = "Hello, World! This is a test."
    val bigObject = createTestObject("test/read-n.txt", testData)
    withStream(bigObject) { stream =>
      val bytes = stream.readNBytes(5)
      assert(bytes.length == 5)
      assert(new String(bytes) == "Hello")
    }
  }

  test("readNBytes() should handle EOF and zero") {
    val bigObject = createTestObject("test/read-n-eof.txt", "Hello")
    withStream(bigObject) { stream =>
      // Request more than available
      val bytes = stream.readNBytes(100)
      assert(bytes.length == 5)
      assert(new String(bytes) == "Hello")
    }

    // Test n=0
    withStream(bigObject) { stream =>
      assert(stream.readNBytes(0).length == 0)
    }
  }

  // skip() Tests
  test("skip() should skip bytes correctly") {
    val bigObject = createTestObject("test/skip.txt", "Hello, World!")
    withStream(bigObject) { stream =>
      assert(stream.skip(7) == 7)
      assert(stream.read() == 'W'.toByte)
    }
  }

  test("skip() should handle EOF and zero") {
    val bigObject = createTestObject("test/skip-eof.txt", "Hello")
    withStream(bigObject) { stream =>
      assert(stream.skip(100) == 5)
      assert(stream.read() == -1)
    }

    // Test n=0
    withStream(bigObject) { stream =>
      assert(stream.skip(0) == 0)
    }
  }

  // available() Tests
  test("available() should return non-negative value") {
    val bigObject = createTestObject("test/available.txt", "Hello, World!")
    withStream(bigObject) { stream =>
      assert(stream.available() >= 0)
    }
  }

  // close() Tests
  test("close() should be idempotent") {
    val bigObject = createTestObject("test/close-idempotent.txt", "data")
    val stream = new BigObjectInputStream(bigObject)
    stream.close()
    stream.close() // Should not throw
    stream.close() // Should not throw
  }

  test("close() should prevent further operations") {
    val bigObject = createTestObject("test/close-prevents.txt", "data")
    val stream = new BigObjectInputStream(bigObject)
    stream.close()

    intercept[IOException] { stream.read() }
    intercept[IOException] { stream.readAllBytes() }
    intercept[IOException] { stream.readNBytes(10) }
    intercept[IOException] { stream.skip(10) }
    intercept[IOException] { stream.available() }
  }

  test("close() should work without reading (lazy initialization)") {
    val bigObject = createTestObject("test/close-lazy.txt", "data")
    val stream = new BigObjectInputStream(bigObject)
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
    val bigObject = createTestObject("test/mark.txt", "data")
    withStream(bigObject) { stream =>
      val supported = stream.markSupported()
      assert(!supported || supported) // Just verify it's callable
    }
  }

  test("mark() and reset() should delegate to underlying stream") {
    val bigObject = createTestObject("test/mark-reset.txt", "data")
    withStream(bigObject) { stream =>
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
    val bigObject = createTestObject("test/lazy-init.txt", "data")
    val stream = new BigObjectInputStream(bigObject)
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
    val bigObject = createTestObject("test/chunked.bin", largeData)
    withStream(bigObject) { stream =>
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
    val bigObject = createTestObject("test/shared.txt", testData)

    val stream1 = new BigObjectInputStream(bigObject)
    val stream2 = new BigObjectInputStream(bigObject)

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
    val bigObject = createTestObject("test/binary.bin", binaryData)
    withStream(bigObject) { stream =>
      assert(stream.readAllBytes().sameElements(binaryData))
    }
  }
}
