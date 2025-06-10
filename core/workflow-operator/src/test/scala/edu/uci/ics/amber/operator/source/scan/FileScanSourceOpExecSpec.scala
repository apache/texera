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

package edu.uci.ics.amber.operator.source.scan

import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, FileOutputStream, IOException}
import java.util.zip.{ZipEntry, ZipOutputStream}

class FileScanSourceOpExecSpec extends AnyFlatSpec with Matchers {

  "FileScanSourceOpExec" should "validate binary file size correctly" in {
    // Create a temporary file larger than 2GB
    val tempFile = File.createTempFile("large", ".bin")
    tempFile.deleteOnExit()

    // Write 2GB + 1 byte to the file
    val fos = new FileOutputStream(tempFile)
    val buffer = new Array[Byte](1024 * 1024) // 1MB buffer
    for (_ <- 0 until 2048) { // Write 2GB
      fos.write(buffer)
    }
    fos.write(1) // Write one more byte
    fos.close()

    // Create FileScanSourceOpDesc with BINARY type
    val desc = new FileScanSourceOpDesc()
    desc.fileName = Some(tempFile.toURI.toString)
    desc.attributeType = FileAttributeType.BINARY
    desc.fileEncoding = FileDecodingMethod.UTF_8
    desc.fileScanOffset = None
    desc.fileScanLimit = None

    // Create executor and expect exception
    val executor = new FileScanSourceOpExec(objectMapper.writeValueAsString(desc))
    val exception = intercept[IOException] {
      executor.produceTuple()
    }
    exception.getMessage should include("exceeds 2GB")
  }

  it should "validate zip file contents size correctly" in {
    // Create a temporary zip file
    val tempZipFile = File.createTempFile("large", ".zip")
    tempZipFile.deleteOnExit()

    // Create a zip file with an entry larger than 2GB
    val zos = new ZipOutputStream(new FileOutputStream(tempZipFile))
    zos.putNextEntry(new ZipEntry("large.bin"))

    // Write 2GB + 1 byte to the zip entry
    val buffer = new Array[Byte](1024 * 1024) // 1MB buffer
    for (_ <- 0 until 2048) { // Write 2GB
      zos.write(buffer)
    }
    zos.write(1) // Write one more byte
    zos.closeEntry()
    zos.close()

    // Create FileScanSourceOpDesc with BINARY type
    val desc = new FileScanSourceOpDesc()
    desc.fileName = Some(tempZipFile.toURI.toString)
    desc.attributeType = FileAttributeType.BINARY
    desc.fileEncoding = FileDecodingMethod.UTF_8
    desc.fileScanOffset = None
    desc.fileScanLimit = None

    // Serialize to JSON and set extract=true
    val descJson = objectMapper
      .readTree(objectMapper.writeValueAsString(desc))
      .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    descJson.put("extract", true)
    val descString = descJson.toString

    // Create executor and expect exception
    val executor = new FileScanSourceOpExec(descString)
    val exception = intercept[IOException] {
      executor.produceTuple()
    }
    exception.getMessage should include("exceeds 2GB")
  }

  it should "accept binary files under 2GB" in {
    // Create a temporary file smaller than 2GB
    val tempFile = File.createTempFile("small", ".bin")
    tempFile.deleteOnExit()

    // Write 1MB to the file
    val fos = new FileOutputStream(tempFile)
    val buffer = new Array[Byte](1024 * 1024) // 1MB buffer
    fos.write(buffer)
    fos.close()

    // Create FileScanSourceOpDesc with BINARY type
    val desc = new FileScanSourceOpDesc()
    desc.fileName = Some(tempFile.toURI.toString)
    desc.attributeType = FileAttributeType.BINARY
    desc.fileEncoding = FileDecodingMethod.UTF_8
    desc.fileScanOffset = None
    desc.fileScanLimit = None

    // Create executor and expect no exception
    val executor = new FileScanSourceOpExec(objectMapper.writeValueAsString(desc))
    noException should be thrownBy executor.produceTuple()
  }
}
