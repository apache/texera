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

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.parseField
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.apache.commons.io.IOUtils.toByteArray
import java.io._
import java.net.URI
import java.nio.ByteBuffer
import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream

class FileScanSourceOpExec private[scan] (
    descString: String
) extends SourceOperatorExecutor {
  private val desc: FileScanSourceOpDesc =
    objectMapper.readValue(descString, classOf[FileScanSourceOpDesc])

  // Size of each chunk when reading large files (1GB)
  private val BufferSizeBytes: Int = 1 * 1024 * 1024 * 1024

  /**
    * Reads an InputStream into a List of ByteBuffers.
    * This allows handling files up to 1TB (1GB * 1000 sub-columns).
    *
    * @param input the input stream to read
    * @return a List of ByteBuffers containing the data
    */
  private def readToByteBuffers(input: InputStream): List[ByteBuffer] = {
    val buffers = ArrayBuffer.empty[ByteBuffer]
    val buffer = new Array[Byte](BufferSizeBytes)
    Iterator
      .continually(input.read(buffer))
      .takeWhile(_ != -1)
      .filter(_ > 0)
      .foreach { bytesRead =>
        val byteBuffer = ByteBuffer.allocate(bytesRead)
        byteBuffer.put(buffer, 0, bytesRead)
        byteBuffer.flip() // Prepare for reading
        buffers += byteBuffer
      }

    buffers.toList
  }

  @throws[IOException]
  override def produceTuple(): Iterator[TupleLike] = {
    val is: InputStream =
      DocumentFactory.openReadonlyDocument(new URI(desc.fileName.get)).asInputStream()

    val closeables = mutable.ArrayBuffer.empty[AutoCloseable]
    var zipIn: ZipArchiveInputStream = null
    var archiveStream: InputStream = null
    if (desc.extract) {
      zipIn = new ArchiveStreamFactory()
        .createArchiveInputStream(new BufferedInputStream(is))
        .asInstanceOf[ZipArchiveInputStream]
      archiveStream = zipIn
      closeables += zipIn
    } else {
      archiveStream = is
      closeables += is
    }

    var filenameIt: Iterator[String] = Iterator.empty
    val fileEntries: Iterator[InputStream] = {
      if (desc.extract) {
        val (it1, it2) = Iterator
          .continually(zipIn.getNextEntry)
          .takeWhile(_ != null)
          .filterNot(_.getName.startsWith("__MACOSX"))
          .duplicate
        filenameIt = it1.map(_.getName)
        it2.map(_ => zipIn)
      } else {
        Iterator(archiveStream)
      }
    }

    val rawIterator: Iterator[TupleLike] =
      if (desc.attributeType.isSingle) {
        fileEntries.zipAll(filenameIt, null, null).map {
          case (entry, fileName) =>
            val fields: mutable.ListBuffer[Any] = mutable.ListBuffer()
            if (desc.outputFileName) {
              fields.addOne(fileName)
            }
            fields.addOne(desc.attributeType match {
              case FileAttributeType.SINGLE_STRING =>
                new String(toByteArray(entry), desc.fileEncoding.getCharset)
              case _ =>
                val buffers = readToByteBuffers(entry)
                parseField(buffers, desc.attributeType.getType)
            })
            TupleLike(fields.toSeq: _*)
        }
      } else {
        fileEntries.flatMap(entry =>
          new BufferedReader(new InputStreamReader(entry, desc.fileEncoding.getCharset))
            .lines()
            .iterator()
            .asScala
            .slice(
              desc.fileScanOffset.getOrElse(0),
              desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
            )
            .map(line => {
              TupleLike(desc.attributeType match {
                case FileAttributeType.SINGLE_STRING => line
                case _                               => parseField(line, desc.attributeType.getType)
              })
            })
        )
      }

    new AutoClosingIterator(rawIterator, () => closeables.foreach(_.close()))
  }
}
