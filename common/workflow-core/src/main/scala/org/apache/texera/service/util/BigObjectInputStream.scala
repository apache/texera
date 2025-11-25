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

import java.io.InputStream

/**
  * InputStream for reading BigObject data from S3.
  *
  * The underlying S3 download is lazily initialized on first read.
  * The stream will fail if the S3 object doesn't exist when read is attempted.
  *
  * Usage:
  * {{{
  *   val bigObject: BigObject = ...
  *   try (val in = new BigObjectInputStream(bigObject)) {
  *     val bytes = in.readAllBytes()
  *   }
  * }}}
  */
class BigObjectInputStream(bigObject: BigObject) extends InputStream {

  require(bigObject != null, "BigObject cannot be null")

  // Lazy initialization - downloads only when first read() is called
  private lazy val underlying: InputStream =
    S3StorageClient.downloadObject(bigObject.getBucketName, bigObject.getObjectKey)

  @volatile private var closed = false

  override def read(): Int = whenOpen(underlying.read())

  override def read(b: Array[Byte], off: Int, len: Int): Int =
    whenOpen(underlying.read(b, off, len))

  override def readAllBytes(): Array[Byte] = whenOpen(underlying.readAllBytes())

  override def readNBytes(n: Int): Array[Byte] = whenOpen(underlying.readNBytes(n))

  override def skip(n: Long): Long = whenOpen(underlying.skip(n))

  override def available(): Int = whenOpen(underlying.available())

  override def close(): Unit = {
    if (!closed) {
      closed = true
      if (underlying != null) { // Only close if initialized
        underlying.close()
      }
    }
  }

  override def markSupported(): Boolean = whenOpen(underlying.markSupported())

  override def mark(readlimit: Int): Unit = whenOpen(underlying.mark(readlimit))

  override def reset(): Unit = whenOpen(underlying.reset())

  private def whenOpen[T](f: => T): T = {
    if (closed) throw new java.io.IOException("Stream is closed")
    f
  }
}
