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

import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.commons.io.IOUtils.toByteArray
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.parseField
import org.apache.texera.amber.core.tuple.LargeBinary
import org.apache.texera.amber.core.tuple.TupleLike
import org.apache.texera.amber.operator.source.scan.{
  AutoClosingIterator,
  FileAttributeType,
  FileDecodingMethod
}
import org.apache.texera.service.util.LargeBinaryOutputStream

import java.io._
import java.net.URI
import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala

private[file] object FileScanUtils {
  def createTuplesFromFile(
      fileName: String,
      displayFileName: String,
      attributeType: FileAttributeType,
      fileEncoding: FileDecodingMethod,
      extract: Boolean,
      outputFileName: Boolean,
      fileScanOffset: Option[Int],
      fileScanLimit: Option[Int]
  ): Iterator[TupleLike] = {
    val inputStream = DocumentFactory.openReadonlyDocument(new URI(fileName)).asInputStream()

    val closeables = mutable.ArrayBuffer.empty[AutoCloseable]
    var zipIn: ZipArchiveInputStream = null
    val archiveStream: InputStream =
      if (extract) {
        zipIn = new ArchiveStreamFactory()
          .createArchiveInputStream(new BufferedInputStream(inputStream))
          .asInstanceOf[ZipArchiveInputStream]
        closeables += zipIn
        zipIn
      } else {
        closeables += inputStream
        inputStream
      }

    var filenameIt: Iterator[String] = Iterator.empty
    val fileEntries: Iterator[InputStream] =
      if (extract) {
        val (it1, it2) = Iterator
          .continually(zipIn.getNextEntry)
          .takeWhile(_ != null)
          .filterNot(_.getName.startsWith("__MACOSX"))
          .duplicate
        filenameIt = it1.map(_.getName)
        it2.map(_ => zipIn)
      } else {
        filenameIt = Iterator.single(displayFileName)
        Iterator(archiveStream)
      }

    val rawIterator: Iterator[TupleLike] =
      if (attributeType.isSingle) {
        fileEntries.zipAll(filenameIt, null, null).map {
          case (entry, entryFileName) =>
            val fields = mutable.ListBuffer.empty[Any]
            if (outputFileName) {
              fields += entryFileName
            }
            fields += (attributeType match {
              case FileAttributeType.SINGLE_STRING =>
                new String(toByteArray(entry), fileEncoding.getCharset)
              case FileAttributeType.LARGE_BINARY =>
                val largeBinary = new LargeBinary()
                val out = new LargeBinaryOutputStream(largeBinary)
                try {
                  val buffer = new Array[Byte](8192)
                  var bytesRead = entry.read(buffer)
                  while (bytesRead != -1) {
                    out.write(buffer, 0, bytesRead)
                    bytesRead = entry.read(buffer)
                  }
                } finally {
                  out.close()
                }
                largeBinary
              case _ => parseField(toByteArray(entry), attributeType.getType)
            })
            TupleLike(fields.toSeq: _*)
        }
      } else {
        fileEntries.flatMap(entry =>
          new BufferedReader(new InputStreamReader(entry, fileEncoding.getCharset))
            .lines()
            .iterator()
            .asScala
            .slice(
              fileScanOffset.getOrElse(0),
              fileScanOffset.getOrElse(0) + fileScanLimit.getOrElse(Int.MaxValue)
            )
            .map(line =>
              TupleLike(attributeType match {
                case FileAttributeType.SINGLE_STRING => line
                case _                               => parseField(line, attributeType.getType)
              })
            )
        )
      }

    new AutoClosingIterator(rawIterator, () => closeables.foreach(_.close()))
  }

  def createTuplesFromFile(
      fileName: String,
      attributeType: FileAttributeType,
      fileEncoding: FileDecodingMethod,
      extract: Boolean,
      outputFileName: Boolean,
      fileScanOffset: Option[Int],
      fileScanLimit: Option[Int]
  ): Iterator[TupleLike] = {
    createTuplesFromFile(
      fileName = fileName,
      displayFileName = fileName,
      attributeType = attributeType,
      fileEncoding = fileEncoding,
      extract = extract,
      outputFileName = outputFileName,
      fileScanOffset = fileScanOffset,
      fileScanLimit = fileScanLimit
    )
  }
}
