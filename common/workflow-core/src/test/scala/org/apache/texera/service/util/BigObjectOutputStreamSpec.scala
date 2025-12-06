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

import org.apache.texera.amber.core.tuple.BigObject
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException
import scala.util.Random

class BigObjectOutputStreamSpec
    extends AnyFunSuite
    with S3StorageTestBase
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val testBucketName = "test-big-object-output-stream"

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
  private def createBigObject(key: String): BigObject =
    new BigObject(s"s3://$testBucketName/$key")

  private def generateRandomData(size: Int): Array[Byte] =
    Array.fill[Byte](size)((Random.nextInt(256) - 128).toByte)

  private def withStream[T](bigObject: BigObject)(f: BigObjectOutputStream => T): T = {
    val stream = new BigObjectOutputStream(bigObject)
    try f(stream)
    finally stream.close()
  }

  private def readBack(bigObject: BigObject): Array[Byte] = {
    val inputStream = new BigObjectInputStream(bigObject)
    try inputStream.readAllBytes()
    finally inputStream.close()
  }

  private def writeAndVerify(key: String, data: Array[Byte]): Unit = {
    val bigObject = createBigObject(key)
    withStream(bigObject)(_.write(data, 0, data.length))
    assert(readBack(bigObject).sameElements(data))
  }

  // === Constructor Tests ===
  test("should reject null BigObject") {
    val exception = intercept[IllegalArgumentException](new BigObjectOutputStream(null))
    assert(exception.getMessage.contains("BigObject cannot be null"))
  }

  // === Basic Write Tests ===
  test("should write single bytes correctly") {
    val bigObject = createBigObject("test/single-bytes.txt")
    withStream(bigObject) { stream =>
      "Hello".foreach(c => stream.write(c.toByte))
    }
    assert(new String(readBack(bigObject)) == "Hello")
  }

  test("should write byte arrays correctly") {
    val testData = "Hello, World!".getBytes
    writeAndVerify("test/array-write.txt", testData)
  }

  test("should handle partial writes with offset and length") {
    val testData = "Hello, World!".getBytes
    val bigObject = createBigObject("test/partial-write.txt")

    withStream(bigObject) { stream =>
      stream.write(testData, 0, 5) // "Hello"
      stream.write(testData, 7, 5) // "World"
    }

    assert(new String(readBack(bigObject)) == "HelloWorld")
  }

  test("should handle multiple consecutive writes") {
    val bigObject = createBigObject("test/multiple-writes.txt")
    withStream(bigObject) { stream =>
      stream.write("Hello".getBytes)
      stream.write(", ".getBytes)
      stream.write("World!".getBytes)
    }
    assert(new String(readBack(bigObject)) == "Hello, World!")
  }

  // === Stream Lifecycle Tests ===
  test("flush should not throw") {
    val bigObject = createBigObject("test/flush.txt")
    withStream(bigObject) { stream =>
      stream.write("test".getBytes)
      stream.flush()
      stream.write(" data".getBytes)
    }
    assert(new String(readBack(bigObject)) == "test data")
  }

  test("close should be idempotent") {
    val bigObject = createBigObject("test/close-idempotent.txt")
    val stream = new BigObjectOutputStream(bigObject)
    stream.write("data".getBytes)
    stream.close()
    stream.close() // Should not throw
    stream.flush() // Should not throw after close
    assert(new String(readBack(bigObject)) == "data")
  }

  test("close should handle empty stream") {
    val bigObject = createBigObject("test/empty-stream.txt")
    val stream = new BigObjectOutputStream(bigObject)
    stream.close()
    assert(readBack(bigObject).length == 0)
  }

  // === Error Handling ===
  test("write operations should throw IOException when stream is closed") {
    val bigObject = createBigObject("test/closed-stream.txt")
    val stream = new BigObjectOutputStream(bigObject)
    stream.close()

    val ex1 = intercept[IOException](stream.write('A'.toByte))
    assert(ex1.getMessage.contains("Stream is closed"))

    val ex2 = intercept[IOException](stream.write("test".getBytes))
    assert(ex2.getMessage.contains("Stream is closed"))
  }

  // === Large Data Tests ===
  test("should handle large data (1MB)") {
    val largeData = generateRandomData(1024 * 1024)
    writeAndVerify("test/large-1mb.bin", largeData)
  }

  test("should handle very large data (10MB)") {
    val veryLargeData = generateRandomData(10 * 1024 * 1024)
    writeAndVerify("test/large-10mb.bin", veryLargeData)
  }

  test("should handle chunked writes") {
    val totalSize = 1024 * 1024 // 1MB
    val chunkSize = 8 * 1024 // 8KB
    val data = generateRandomData(totalSize)
    val bigObject = createBigObject("test/chunked.bin")

    withStream(bigObject) { stream =>
      data.grouped(chunkSize).foreach(chunk => stream.write(chunk))
    }

    assert(readBack(bigObject).sameElements(data))
  }

  // === Binary Data Tests ===
  test("should preserve all byte values (0-255)") {
    val allBytes = (0 until 256).map(_.toByte).toArray
    writeAndVerify("test/all-bytes.bin", allBytes)
  }

  // === Integration Tests ===
  test("should handle concurrent writes to different objects") {
    val streams = (1 to 3).map { i =>
      val obj = createBigObject(s"test/concurrent-$i.txt")
      val stream = new BigObjectOutputStream(obj)
      (obj, stream, s"Data $i")
    }

    try {
      streams.foreach { case (_, stream, data) => stream.write(data.getBytes) }
    } finally {
      streams.foreach(_._2.close())
    }

    streams.foreach {
      case (obj, _, expected) =>
        assert(new String(readBack(obj)) == expected)
    }
  }

  test("should overwrite existing object") {
    val bigObject = createBigObject("test/overwrite.txt")
    withStream(bigObject)(_.write("original data".getBytes))
    withStream(bigObject)(_.write("new data".getBytes))
    assert(new String(readBack(bigObject)) == "new data")
  }

  test("should handle mixed write operations") {
    val bigObject = createBigObject("test/mixed-writes.txt")
    withStream(bigObject) { stream =>
      stream.write('A'.toByte)
      stream.write(" test ".getBytes)
      stream.write('B'.toByte)
      val data = "Hello, World!".getBytes
      stream.write(data, 7, 6) // "World!"
    }
    assert(new String(readBack(bigObject)) == "A test BWorld!")
  }

  // === Edge Cases ===
  test("should create bucket automatically") {
    val newBucketName = s"new-bucket-${Random.nextInt(10000)}"
    val bigObject = new BigObject(s"s3://$newBucketName/test/auto-create.txt")

    try {
      withStream(bigObject)(_.write("test".getBytes))
      assert(new String(readBack(bigObject)) == "test")
    } finally {
      try S3StorageClient.deleteDirectory(newBucketName, "")
      catch { case _: Exception => /* ignore */ }
    }
  }

  test("should handle rapid open/close cycles") {
    (1 to 10).foreach { i =>
      withStream(createBigObject(s"test/rapid-$i.txt"))(_.write(s"data-$i".getBytes))
    }

    (1 to 10).foreach { i =>
      val result = readBack(createBigObject(s"test/rapid-$i.txt"))
      assert(new String(result) == s"data-$i")
    }
  }
}
