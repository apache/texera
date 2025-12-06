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
import org.scalatest.funsuite.AnyFunSuite

class LargeBinaryManagerSpec extends AnyFunSuite with S3StorageTestBase {

  /** Creates a large binary from string data and returns it. */
  private def createLargeBinary(data: String): LargeBinary = {
    val largeBinary = new LargeBinary()
    val out = new LargeBinaryOutputStream(largeBinary)
    try {
      out.write(data.getBytes)
    } finally {
      out.close()
    }
    largeBinary
  }

  /** Verifies standard bucket name. */
  private def assertStandardBucket(pointer: LargeBinary): Unit = {
    assert(pointer.getBucketName == "texera-large-binaries")
    assert(pointer.getUri.startsWith("s3://texera-large-binaries/"))
  }

  // ========================================
  // LargeBinaryInputStream Tests (Standard Java InputStream)
  // ========================================

  test("LargeBinaryInputStream should read all bytes from stream") {
    val data = "Hello, World! This is a test."
    val largeBinary = createLargeBinary(data)

    val stream = new LargeBinaryInputStream(largeBinary)
    assert(stream.readAllBytes().sameElements(data.getBytes))
    stream.close()

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryInputStream should read exact number of bytes") {
    val largeBinary = createLargeBinary("0123456789ABCDEF")

    val stream = new LargeBinaryInputStream(largeBinary)
    val result = stream.readNBytes(10)

    assert(result.length == 10)
    assert(result.sameElements("0123456789".getBytes))
    stream.close()

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryInputStream should handle reading more bytes than available") {
    val data = "Short"
    val largeBinary = createLargeBinary(data)

    val stream = new LargeBinaryInputStream(largeBinary)
    val result = stream.readNBytes(100)

    assert(result.length == data.length)
    assert(result.sameElements(data.getBytes))
    stream.close()

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryInputStream should support standard single-byte read") {
    val largeBinary = createLargeBinary("ABC")

    val stream = new LargeBinaryInputStream(largeBinary)
    assert(stream.read() == 65) // 'A'
    assert(stream.read() == 66) // 'B'
    assert(stream.read() == 67) // 'C'
    assert(stream.read() == -1) // EOF
    stream.close()

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryInputStream should return -1 at EOF") {
    val largeBinary = createLargeBinary("EOF")

    val stream = new LargeBinaryInputStream(largeBinary)
    stream.readAllBytes() // Read all data
    assert(stream.read() == -1)
    stream.close()

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryInputStream should throw exception when reading from closed stream") {
    val largeBinary = createLargeBinary("test")

    val stream = new LargeBinaryInputStream(largeBinary)
    stream.close()

    assertThrows[java.io.IOException](stream.read())
    assertThrows[java.io.IOException](stream.readAllBytes())

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryInputStream should handle multiple close calls") {
    val largeBinary = createLargeBinary("test")

    val stream = new LargeBinaryInputStream(largeBinary)
    stream.close()
    stream.close() // Should not throw

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryInputStream should read large data correctly") {
    val largeData = Array.fill[Byte](20000)((scala.util.Random.nextInt(256) - 128).toByte)
    val largeBinary = new LargeBinary()
    val out = new LargeBinaryOutputStream(largeBinary)
    try {
      out.write(largeData)
    } finally {
      out.close()
    }

    val stream = new LargeBinaryInputStream(largeBinary)
    val result = stream.readAllBytes()
    assert(result.sameElements(largeData))
    stream.close()

    LargeBinaryManager.deleteAllObjects()
  }

  // ========================================
  // LargeBinaryManager Tests
  // ========================================

  test("LargeBinaryManager should create a large binary") {
    val pointer = createLargeBinary("Test large binary data")

    assertStandardBucket(pointer)
  }

  test("LargeBinaryInputStream should open and read a large binary") {
    val data = "Hello from large binary!"
    val pointer = createLargeBinary(data)

    val stream = new LargeBinaryInputStream(pointer)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(data.getBytes))
  }

  test("LargeBinaryInputStream should fail to open non-existent large binary") {
    val fakeLargeBinary = new LargeBinary("s3://texera-large-binaries/nonexistent/file")
    val stream = new LargeBinaryInputStream(fakeLargeBinary)

    try {
      intercept[Exception] {
        stream.read()
      }
    } finally {
      try { stream.close() }
      catch { case _: Exception => }
    }
  }

  test("LargeBinaryManager should delete all large binaries") {
    val pointer1 = new LargeBinary()
    val out1 = new LargeBinaryOutputStream(pointer1)
    try {
      out1.write("Object 1".getBytes)
    } finally {
      out1.close()
    }

    val pointer2 = new LargeBinary()
    val out2 = new LargeBinaryOutputStream(pointer2)
    try {
      out2.write("Object 2".getBytes)
    } finally {
      out2.close()
    }

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryManager should handle delete with no objects gracefully") {
    LargeBinaryManager.deleteAllObjects() // Should not throw exception
  }

  test("LargeBinaryManager should delete all objects") {
    val pointer1 = createLargeBinary("Test data")
    val pointer2 = createLargeBinary("Test data")

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryManager should create bucket if it doesn't exist") {
    val pointer = createLargeBinary("Test bucket creation")

    assertStandardBucket(pointer)

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryManager should handle large objects correctly") {
    val largeData = Array.fill[Byte](6 * 1024 * 1024)((scala.util.Random.nextInt(256) - 128).toByte)
    val pointer = new LargeBinary()
    val out = new LargeBinaryOutputStream(pointer)
    try {
      out.write(largeData)
    } finally {
      out.close()
    }

    val stream = new LargeBinaryInputStream(pointer)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(largeData))
    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryManager should generate unique URIs for different objects") {
    val testData = "Unique URI test".getBytes
    val pointer1 = new LargeBinary()
    val out1 = new LargeBinaryOutputStream(pointer1)
    try {
      out1.write(testData)
    } finally {
      out1.close()
    }

    val pointer2 = new LargeBinary()
    val out2 = new LargeBinaryOutputStream(pointer2)
    try {
      out2.write(testData)
    } finally {
      out2.close()
    }

    assert(pointer1.getUri != pointer2.getUri)
    assert(pointer1.getObjectKey != pointer2.getObjectKey)

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryInputStream should handle multiple reads from the same large binary") {
    val data = "Multiple reads test data"
    val pointer = createLargeBinary(data)

    val stream1 = new LargeBinaryInputStream(pointer)
    val readData1 = stream1.readAllBytes()
    stream1.close()

    val stream2 = new LargeBinaryInputStream(pointer)
    val readData2 = stream2.readAllBytes()
    stream2.close()

    assert(readData1.sameElements(data.getBytes))
    assert(readData2.sameElements(data.getBytes))

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryManager should properly parse bucket name and object key from large binary") {
    val largeBinary = createLargeBinary("URI parsing test")

    assertStandardBucket(largeBinary)
    assert(largeBinary.getObjectKey.nonEmpty)
    assert(!largeBinary.getObjectKey.startsWith("/"))

    LargeBinaryManager.deleteAllObjects()
  }

  // ========================================
  // Object-Oriented API Tests
  // ========================================

  test("LargeBinary with LargeBinaryOutputStream should create a large binary") {
    val data = "Test data for LargeBinary with LargeBinaryOutputStream"

    val largeBinary = new LargeBinary()
    val out = new LargeBinaryOutputStream(largeBinary)
    try {
      out.write(data.getBytes)
    } finally {
      out.close()
    }

    assertStandardBucket(largeBinary)

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryInputStream constructor should read large binary contents") {
    val data = "Test data for LargeBinaryInputStream constructor"
    val largeBinary = createLargeBinary(data)

    val stream = new LargeBinaryInputStream(largeBinary)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(data.getBytes))

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryOutputStream and LargeBinaryInputStream should work together end-to-end") {
    val data = "End-to-end test data"

    // Create using streaming API
    val largeBinary = new LargeBinary()
    val out = new LargeBinaryOutputStream(largeBinary)
    try {
      out.write(data.getBytes)
    } finally {
      out.close()
    }

    // Read using standard constructor
    val stream = new LargeBinaryInputStream(largeBinary)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(data.getBytes))

    LargeBinaryManager.deleteAllObjects()
  }

  // ========================================
  // LargeBinaryOutputStream Tests (New Symmetric API)
  // ========================================

  test("LargeBinaryOutputStream should write and upload data to S3") {
    val data = "Test data for LargeBinaryOutputStream"

    val largeBinary = new LargeBinary()
    val outStream = new LargeBinaryOutputStream(largeBinary)
    outStream.write(data.getBytes)
    outStream.close()

    assertStandardBucket(largeBinary)

    // Verify data can be read back
    val inStream = new LargeBinaryInputStream(largeBinary)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements(data.getBytes))

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryOutputStream should create large binary") {
    val data = "Database registration test"

    val largeBinary = new LargeBinary()
    val outStream = new LargeBinaryOutputStream(largeBinary)
    outStream.write(data.getBytes)
    outStream.close()

    assertStandardBucket(largeBinary)

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryOutputStream should handle large data correctly") {
    val largeData = Array.fill[Byte](8 * 1024 * 1024)((scala.util.Random.nextInt(256) - 128).toByte)

    val largeBinary = new LargeBinary()
    val outStream = new LargeBinaryOutputStream(largeBinary)
    outStream.write(largeData)
    outStream.close()

    // Verify data integrity
    val inStream = new LargeBinaryInputStream(largeBinary)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements(largeData))

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryOutputStream should handle multiple writes") {
    val largeBinary = new LargeBinary()
    val outStream = new LargeBinaryOutputStream(largeBinary)
    outStream.write("Hello ".getBytes)
    outStream.write("World".getBytes)
    outStream.write("!".getBytes)
    outStream.close()

    val inStream = new LargeBinaryInputStream(largeBinary)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements("Hello World!".getBytes))

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryOutputStream should throw exception when writing to closed stream") {
    val largeBinary = new LargeBinary()
    val outStream = new LargeBinaryOutputStream(largeBinary)
    outStream.write("test".getBytes)
    outStream.close()

    assertThrows[java.io.IOException](outStream.write("more".getBytes))

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinaryOutputStream should handle close() being called multiple times") {
    val largeBinary = new LargeBinary()
    val outStream = new LargeBinaryOutputStream(largeBinary)
    outStream.write("test".getBytes)
    outStream.close()
    outStream.close() // Should not throw

    LargeBinaryManager.deleteAllObjects()
  }

  test("New LargeBinary() constructor should create unique URIs") {
    val largeBinary1 = new LargeBinary()
    val largeBinary2 = new LargeBinary()

    assert(largeBinary1.getUri != largeBinary2.getUri)
    assert(largeBinary1.getObjectKey != largeBinary2.getObjectKey)

    LargeBinaryManager.deleteAllObjects()
  }

  test("LargeBinary() and LargeBinaryOutputStream API should be symmetric with input") {
    val data = "Symmetric API test"

    // Write using new symmetric API
    val largeBinary = new LargeBinary()
    val outStream = new LargeBinaryOutputStream(largeBinary)
    outStream.write(data.getBytes)
    outStream.close()

    // Read using symmetric API
    val inStream = new LargeBinaryInputStream(largeBinary)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements(data.getBytes))

    LargeBinaryManager.deleteAllObjects()
  }
}
