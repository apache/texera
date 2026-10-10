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

package org.apache.texera.amber.operator.source.scan.file

import org.apache.texera.amber.operator.source.scan.{FileAttributeType, FileDecodingMethod}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{BufferedOutputStream, FileOutputStream, IOException, InputStream}
import java.nio.file.{Files, Path}
import java.util.zip.{ZipEntry, ZipOutputStream}

class FileScanUtilsSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private val tempFiles = scala.collection.mutable.ArrayBuffer.empty[Path]

  private def makeZip(entries: (String, String)*): String = {
    val path = Files.createTempFile("filescanutils-", ".zip")
    tempFiles += path
    val zipOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile)))
    try {
      entries.foreach {
        case (name, content) =>
          zipOut.putNextEntry(new ZipEntry(name))
          zipOut.write(content.getBytes("UTF-8"))
          zipOut.closeEntry()
      }
    } finally {
      zipOut.close()
    }
    path.toFile.toURI.toString
  }

  private def makeTextFile(content: String): String = {
    val path = Files.createTempFile("filescanutils-", ".txt")
    tempFiles += path
    Files.write(path, content.getBytes("UTF-8"))
    path.toFile.toURI.toString
  }

  override def afterAll(): Unit = {
    tempFiles.foreach(Files.deleteIfExists)
    super.afterAll()
  }

  private def contents(tuples: Seq[org.apache.texera.amber.core.tuple.TupleLike]): Seq[Any] =
    tuples.map(_.getFields.head)

  "FileScanUtils.createTuplesFromFile" should
    "extract every zip entry as a single-string tuple" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeZip("a.txt" -> "Content A", "b.txt" -> "Content B"),
        displayFileName = "ignored-when-extracting",
        attributeType = FileAttributeType.SINGLE_STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = true,
        outputFileName = false,
        fileScanOffset = None,
        fileScanLimit = None
      )
      .toSeq
    assert(tuples.size == 2)
    assert(contents(tuples).toSet == Set("Content A", "Content B"))
  }

  it should "drop __MACOSX metadata entries when extracting" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeZip("real.txt" -> "keep me", "__MACOSX/._real.txt" -> "junk"),
        displayFileName = "d",
        attributeType = FileAttributeType.SINGLE_STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = true,
        outputFileName = false,
        fileScanOffset = None,
        fileScanLimit = None
      )
      .toSeq
    assert(contents(tuples) == Seq("keep me"))
  }

  it should "flat-map each line of an extracted entry for a per-line attribute type" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeZip("lines.txt" -> "l1\nl2\nl3"),
        displayFileName = "d",
        attributeType = FileAttributeType.STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = true,
        outputFileName = false,
        fileScanOffset = None,
        fileScanLimit = None
      )
      .toSeq
    assert(contents(tuples) == Seq("l1", "l2", "l3"))
  }

  it should "skip the offset lines and return all remaining lines when no limit is set" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeTextFile("l1\nl2\nl3\nl4\nl5"),
        displayFileName = "d",
        attributeType = FileAttributeType.STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = false,
        outputFileName = false,
        fileScanOffset = Some(1),
        fileScanLimit = None
      )
      .toSeq
    assert(contents(tuples) == Seq("l2", "l3", "l4", "l5"))
  }

  it should "return every line for a zero offset with no limit" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeTextFile("l1\nl2\nl3\nl4\nl5"),
        displayFileName = "d",
        attributeType = FileAttributeType.STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = false,
        outputFileName = false,
        fileScanOffset = Some(0),
        fileScanLimit = None
      )
      .toSeq
    assert(contents(tuples) == Seq("l1", "l2", "l3", "l4", "l5"))
  }

  it should "return limit lines starting at the offset when both are set" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeTextFile("l1\nl2\nl3\nl4\nl5"),
        displayFileName = "d",
        attributeType = FileAttributeType.STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = false,
        outputFileName = false,
        fileScanOffset = Some(1),
        fileScanLimit = Some(2)
      )
      .toSeq
    assert(contents(tuples) == Seq("l2", "l3"))
  }

  it should "return the first limit lines when only a limit is set" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeTextFile("l1\nl2\nl3\nl4\nl5"),
        displayFileName = "d",
        attributeType = FileAttributeType.STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = false,
        outputFileName = false,
        fileScanOffset = None,
        fileScanLimit = Some(2)
      )
      .toSeq
    assert(contents(tuples) == Seq("l1", "l2"))
  }

  it should "return no tuples when the offset is past the end of the file" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeTextFile("l1\nl2\nl3\nl4\nl5"),
        displayFileName = "d",
        attributeType = FileAttributeType.STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = false,
        outputFileName = false,
        fileScanOffset = Some(99),
        fileScanLimit = None
      )
      .toSeq
    assert(contents(tuples) == Seq.empty)
  }

  it should "return no tuples for an Int.MaxValue offset without overflowing" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeTextFile("l1\nl2\nl3\nl4\nl5"),
        displayFileName = "d",
        attributeType = FileAttributeType.STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = false,
        outputFileName = false,
        fileScanOffset = Some(Int.MaxValue),
        fileScanLimit = None
      )
      .toSeq
    assert(contents(tuples) == Seq.empty)
  }

  it should "apply an offset without a limit to each extracted zip entry independently" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeZip("a.txt" -> "a1\na2", "b.txt" -> "b1\nb2"),
        displayFileName = "d",
        attributeType = FileAttributeType.STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = true,
        outputFileName = false,
        fileScanOffset = Some(1),
        fileScanLimit = None
      )
      .toSeq
    assert(contents(tuples) == Seq("a2", "b2"))
  }

  it should "ignore the offset for a single-tuple attribute type" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeTextFile("l1\nl2\nl3\nl4\nl5"),
        displayFileName = "d",
        attributeType = FileAttributeType.SINGLE_STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = false,
        outputFileName = false,
        fileScanOffset = Some(1),
        fileScanLimit = None
      )
      .toSeq
    assert(contents(tuples) == Seq("l1\nl2\nl3\nl4\nl5"))
  }

  it should "read a file through the seven-argument overload under its own name" in {
    // The short overload exists only to default displayFileName to fileName, so the
    // emitted file-name column is the observation that pins the delegation.
    val uri = makeTextFile("only line")
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = uri,
        attributeType = FileAttributeType.SINGLE_STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = false,
        outputFileName = true,
        fileScanOffset = None,
        fileScanLimit = None
      )
      .toSeq
    assert(tuples.size == 1)
    assert(tuples.head.getFields.toSeq == Seq(uri, "only line"))
  }

  it should "forward the offset and limit through the seven-argument overload" in {
    val tuples = FileScanUtils
      .createTuplesFromFile(
        fileName = makeTextFile("l1\nl2\nl3\nl4\nl5"),
        attributeType = FileAttributeType.STRING,
        fileEncoding = FileDecodingMethod.UTF_8,
        extract = false,
        outputFileName = false,
        fileScanOffset = Some(1),
        fileScanLimit = Some(2)
      )
      .toSeq
    assert(contents(tuples) == Seq("l2", "l3"))
  }

  // -- safeToByteArray: the out-of-memory guard ---------------------------------

  /** A stream whose every read fails with `failure`, to drive safeToByteArray's catch. */
  private def failingStream(failure: Throwable): InputStream =
    new InputStream {
      override def read(): Int = throw failure
      override def read(b: Array[Byte], off: Int, len: Int): Int = throw failure
    }

  "FileScanUtils.safeToByteArray" should
    "translate an out-of-memory read into advice to switch to large binary" in {
    val exception = intercept[RuntimeException] {
      FileScanUtils.safeToByteArray(
        failingStream(new OutOfMemoryError("Requested array size exceeds VM limit")),
        FileAttributeType.BINARY
      )
    }
    assert(
      exception.getMessage ==
        "File exceeds maximum safe memory size for 'binary' type. " +
          "Please use 'large binary' attribute type instead."
    )
  }

  it should "advise splitting or chunking the file for a single-string attribute" in {
    // 'large binary' is meaningless for a text column, so this arm has to give
    // different advice from the binary one.
    val exception = intercept[RuntimeException] {
      FileScanUtils.safeToByteArray(
        failingStream(new OutOfMemoryError("Requested array size exceeds VM limit")),
        FileAttributeType.SINGLE_STRING
      )
    }
    assert(
      exception.getMessage ==
        "File exceeds maximum safe memory size for 'single string' type. " +
          "Please split the file or use a chunked reading method."
    )
  }

  it should "fall back to a generic hint for any other attribute type" in {
    val exception = intercept[RuntimeException] {
      FileScanUtils.safeToByteArray(
        failingStream(new OutOfMemoryError("Requested array size exceeds VM limit")),
        FileAttributeType.STRING
      )
    }
    assert(
      exception.getMessage ==
        "File exceeds maximum safe memory size for 'string' type. " +
          "File is too large to fit in memory."
    )
  }

  it should "treat an IllegalArgumentException from the stream as an over-size failure too" in {
    // An over-large allocation surfaces as IllegalArgumentException rather than
    // OutOfMemoryError on some streams, so the guard catches both.
    val exception = intercept[RuntimeException] {
      FileScanUtils.safeToByteArray(
        failingStream(new IllegalArgumentException("negative capacity")),
        FileAttributeType.BINARY
      )
    }
    assert(exception.getMessage.startsWith("File exceeds maximum safe memory size"))
  }

  it should "let an unrelated read failure propagate untouched" in {
    // Only the two over-size signals are translated; a genuine I/O failure must
    // reach the caller as itself rather than as a misleading memory diagnosis.
    val exception = intercept[IOException] {
      FileScanUtils.safeToByteArray(
        failingStream(new IOException("disk went away")),
        FileAttributeType.BINARY
      )
    }
    assert(exception.getMessage == "disk went away")
  }
}
