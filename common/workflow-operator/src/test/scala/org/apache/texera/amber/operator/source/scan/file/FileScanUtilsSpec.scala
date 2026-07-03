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

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.file.{Files, Path}
import java.util.zip.{ZipEntry, ZipOutputStream}

class FileScanUtilsSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private val zips = scala.collection.mutable.ArrayBuffer.empty[Path]

  private def makeZip(entries: (String, String)*): String = {
    val path = Files.createTempFile("filescanutils-", ".zip")
    zips += path
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

  override def afterAll(): Unit = {
    zips.foreach(Files.deleteIfExists)
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
}
