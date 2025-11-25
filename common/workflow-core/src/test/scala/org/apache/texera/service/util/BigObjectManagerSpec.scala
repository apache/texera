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
import org.scalatest.funsuite.AnyFunSuite

class BigObjectManagerSpec extends AnyFunSuite with S3StorageTestBase {

  /** Creates a big object from string data and returns it. */
  private def createBigObject(data: String): BigObject = {
    val bigObject = new BigObject()
    val out = new BigObjectOutputStream(bigObject)
    try {
      out.write(data.getBytes)
    } finally {
      out.close()
    }
    bigObject
  }

  /** Verifies standard bucket name. */
  private def assertStandardBucket(pointer: BigObject): Unit = {
    assert(pointer.getBucketName == "texera-big-objects")
    assert(pointer.getUri.startsWith("s3://texera-big-objects/"))
  }

  // ========================================
  // BigObjectInputStream Tests (Standard Java InputStream)
  // ========================================

  test("BigObjectInputStream should read all bytes from stream") {
    val data = "Hello, World! This is a test."
    val bigObject = createBigObject(data)

    val stream = new BigObjectInputStream(bigObject)
    assert(stream.readAllBytes().sameElements(data.getBytes))
    stream.close()

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectInputStream should read exact number of bytes") {
    val bigObject = createBigObject("0123456789ABCDEF")

    val stream = new BigObjectInputStream(bigObject)
    val result = stream.readNBytes(10)

    assert(result.length == 10)
    assert(result.sameElements("0123456789".getBytes))
    stream.close()

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectInputStream should handle reading more bytes than available") {
    val data = "Short"
    val bigObject = createBigObject(data)

    val stream = new BigObjectInputStream(bigObject)
    val result = stream.readNBytes(100)

    assert(result.length == data.length)
    assert(result.sameElements(data.getBytes))
    stream.close()

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectInputStream should support standard single-byte read") {
    val bigObject = createBigObject("ABC")

    val stream = new BigObjectInputStream(bigObject)
    assert(stream.read() == 65) // 'A'
    assert(stream.read() == 66) // 'B'
    assert(stream.read() == 67) // 'C'
    assert(stream.read() == -1) // EOF
    stream.close()

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectInputStream should return -1 at EOF") {
    val bigObject = createBigObject("EOF")

    val stream = new BigObjectInputStream(bigObject)
    stream.readAllBytes() // Read all data
    assert(stream.read() == -1)
    stream.close()

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectInputStream should throw exception when reading from closed stream") {
    val bigObject = createBigObject("test")

    val stream = new BigObjectInputStream(bigObject)
    stream.close()

    assertThrows[java.io.IOException](stream.read())
    assertThrows[java.io.IOException](stream.readAllBytes())

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectInputStream should handle multiple close calls") {
    val bigObject = createBigObject("test")

    val stream = new BigObjectInputStream(bigObject)
    stream.close()
    stream.close() // Should not throw

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectInputStream should read large data correctly") {
    val largeData = Array.fill[Byte](20000)((scala.util.Random.nextInt(256) - 128).toByte)
    val bigObject = new BigObject()
    val out = new BigObjectOutputStream(bigObject)
    try {
      out.write(largeData)
    } finally {
      out.close()
    }

    val stream = new BigObjectInputStream(bigObject)
    val result = stream.readAllBytes()
    assert(result.sameElements(largeData))
    stream.close()

    BigObjectManager.deleteAllObjects()
  }

  // ========================================
  // BigObjectManager Tests
  // ========================================

  test("BigObjectManager should create a big object") {
    val pointer = createBigObject("Test big object data")

    assertStandardBucket(pointer)
  }

  test("BigObjectInputStream should open and read a big object") {
    val data = "Hello from big object!"
    val pointer = createBigObject(data)

    val stream = new BigObjectInputStream(pointer)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(data.getBytes))
  }

  test("BigObjectInputStream should fail to open non-existent big object") {
    val fakeBigObject = new BigObject("s3://texera-big-objects/nonexistent/file")
    val stream = new BigObjectInputStream(fakeBigObject)

    try {
      intercept[Exception] {
        stream.read()
      }
    } finally {
      try { stream.close() }
      catch { case _: Exception => }
    }
  }

  test("BigObjectManager should delete all big objects") {
    val pointer1 = new BigObject()
    val out1 = new BigObjectOutputStream(pointer1)
    try {
      out1.write("Object 1".getBytes)
    } finally {
      out1.close()
    }

    val pointer2 = new BigObject()
    val out2 = new BigObjectOutputStream(pointer2)
    try {
      out2.write("Object 2".getBytes)
    } finally {
      out2.close()
    }

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectManager should handle delete with no objects gracefully") {
    BigObjectManager.deleteAllObjects() // Should not throw exception
  }

  test("BigObjectManager should delete all objects") {
    val pointer1 = createBigObject("Test data")
    val pointer2 = createBigObject("Test data")

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectManager should create bucket if it doesn't exist") {
    val pointer = createBigObject("Test bucket creation")

    assertStandardBucket(pointer)

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectManager should handle large objects correctly") {
    val largeData = Array.fill[Byte](6 * 1024 * 1024)((scala.util.Random.nextInt(256) - 128).toByte)
    val pointer = new BigObject()
    val out = new BigObjectOutputStream(pointer)
    try {
      out.write(largeData)
    } finally {
      out.close()
    }

    val stream = new BigObjectInputStream(pointer)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(largeData))
    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectManager should generate unique URIs for different objects") {
    val testData = "Unique URI test".getBytes
    val pointer1 = new BigObject()
    val out1 = new BigObjectOutputStream(pointer1)
    try {
      out1.write(testData)
    } finally {
      out1.close()
    }

    val pointer2 = new BigObject()
    val out2 = new BigObjectOutputStream(pointer2)
    try {
      out2.write(testData)
    } finally {
      out2.close()
    }

    assert(pointer1.getUri != pointer2.getUri)
    assert(pointer1.getObjectKey != pointer2.getObjectKey)

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectInputStream should handle multiple reads from the same big object") {
    val data = "Multiple reads test data"
    val pointer = createBigObject(data)

    val stream1 = new BigObjectInputStream(pointer)
    val readData1 = stream1.readAllBytes()
    stream1.close()

    val stream2 = new BigObjectInputStream(pointer)
    val readData2 = stream2.readAllBytes()
    stream2.close()

    assert(readData1.sameElements(data.getBytes))
    assert(readData2.sameElements(data.getBytes))

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectManager should properly parse bucket name and object key from big object") {
    val bigObject = createBigObject("URI parsing test")

    assertStandardBucket(bigObject)
    assert(bigObject.getObjectKey.nonEmpty)
    assert(!bigObject.getObjectKey.startsWith("/"))

    BigObjectManager.deleteAllObjects()
  }

  // ========================================
  // Object-Oriented API Tests
  // ========================================

  test("BigObject with BigObjectOutputStream should create a big object") {
    val data = "Test data for BigObject with BigObjectOutputStream"

    val bigObject = new BigObject()
    val out = new BigObjectOutputStream(bigObject)
    try {
      out.write(data.getBytes)
    } finally {
      out.close()
    }

    assertStandardBucket(bigObject)

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectInputStream constructor should read big object contents") {
    val data = "Test data for BigObjectInputStream constructor"
    val bigObject = createBigObject(data)

    val stream = new BigObjectInputStream(bigObject)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(data.getBytes))

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectOutputStream and BigObjectInputStream should work together end-to-end") {
    val data = "End-to-end test data"

    // Create using streaming API
    val bigObject = new BigObject()
    val out = new BigObjectOutputStream(bigObject)
    try {
      out.write(data.getBytes)
    } finally {
      out.close()
    }

    // Read using standard constructor
    val stream = new BigObjectInputStream(bigObject)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(data.getBytes))

    BigObjectManager.deleteAllObjects()
  }

  // ========================================
  // BigObjectOutputStream Tests (New Symmetric API)
  // ========================================

  test("BigObjectOutputStream should write and upload data to S3") {
    val data = "Test data for BigObjectOutputStream"

    val bigObject = new BigObject()
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write(data.getBytes)
    outStream.close()

    assertStandardBucket(bigObject)

    // Verify data can be read back
    val inStream = new BigObjectInputStream(bigObject)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements(data.getBytes))

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectOutputStream should create big object") {
    val data = "Database registration test"

    val bigObject = new BigObject()
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write(data.getBytes)
    outStream.close()

    assertStandardBucket(bigObject)

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectOutputStream should handle large data correctly") {
    val largeData = Array.fill[Byte](8 * 1024 * 1024)((scala.util.Random.nextInt(256) - 128).toByte)

    val bigObject = new BigObject()
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write(largeData)
    outStream.close()

    // Verify data integrity
    val inStream = new BigObjectInputStream(bigObject)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements(largeData))

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectOutputStream should handle multiple writes") {
    val bigObject = new BigObject()
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write("Hello ".getBytes)
    outStream.write("World".getBytes)
    outStream.write("!".getBytes)
    outStream.close()

    val inStream = new BigObjectInputStream(bigObject)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements("Hello World!".getBytes))

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectOutputStream should throw exception when writing to closed stream") {
    val bigObject = new BigObject()
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write("test".getBytes)
    outStream.close()

    assertThrows[java.io.IOException](outStream.write("more".getBytes))

    BigObjectManager.deleteAllObjects()
  }

  test("BigObjectOutputStream should handle close() being called multiple times") {
    val bigObject = new BigObject()
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write("test".getBytes)
    outStream.close()
    outStream.close() // Should not throw

    BigObjectManager.deleteAllObjects()
  }

  test("New BigObject() constructor should create unique URIs") {
    val bigObject1 = new BigObject()
    val bigObject2 = new BigObject()

    assert(bigObject1.getUri != bigObject2.getUri)
    assert(bigObject1.getObjectKey != bigObject2.getObjectKey)

    BigObjectManager.deleteAllObjects()
  }

  test("BigObject() and BigObjectOutputStream API should be symmetric with input") {
    val data = "Symmetric API test"

    // Write using new symmetric API
    val bigObject = new BigObject()
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write(data.getBytes)
    outStream.close()

    // Read using symmetric API
    val inStream = new BigObjectInputStream(bigObject)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements(data.getBytes))

    BigObjectManager.deleteAllObjects()
  }
}
