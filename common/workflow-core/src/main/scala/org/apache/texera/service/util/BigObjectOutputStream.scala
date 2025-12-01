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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.tuple.BigObject

import java.io.{IOException, OutputStream, PipedInputStream, PipedOutputStream}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
  * OutputStream for streaming BigObject data to S3.
  *
  * Data is uploaded in the background using multipart upload as you write.
  * Call close() to complete the upload and ensure all data is persisted.
  *
  * Usage:
  * {{{
  *   val bigObject = new BigObject()
  *   try (val out = new BigObjectOutputStream(bigObject)) {
  *     out.write(myBytes)
  *   }
  *   // bigObject is now ready to use
  * }}}
  *
  * Note: Not thread-safe. Do not access from multiple threads concurrently.
  *
  * @param bigObject The BigObject reference to write to
  */
class BigObjectOutputStream(bigObject: BigObject) extends OutputStream with LazyLogging {

  private val PIPE_BUFFER_SIZE = 64 * 1024 // 64KB

  require(bigObject != null, "BigObject cannot be null")

  private val bucketName: String = bigObject.getBucketName
  private val objectKey: String = bigObject.getObjectKey
  private implicit val ec: ExecutionContext = ExecutionContext.global

  // Pipe: we write to pipedOut, and S3 reads from pipedIn
  private val pipedIn = new PipedInputStream(PIPE_BUFFER_SIZE)
  private val pipedOut = new PipedOutputStream(pipedIn)

  @volatile private var closed = false
  private val uploadException = new AtomicReference[Option[Throwable]](None)

  // Start background upload immediately
  private val uploadFuture: Future[Unit] = Future {
    try {
      S3StorageClient.createBucketIfNotExist(bucketName)
      S3StorageClient.uploadObject(bucketName, objectKey, pipedIn)
      logger.debug(s"Upload completed: ${bigObject.getUri}")
    } catch {
      case e: Exception =>
        uploadException.set(Some(e))
        logger.error(s"Upload failed: ${bigObject.getUri}", e)
    } finally {
      pipedIn.close()
    }
  }

  override def write(b: Int): Unit = whenOpen(pipedOut.write(b))

  override def write(b: Array[Byte], off: Int, len: Int): Unit =
    whenOpen(pipedOut.write(b, off, len))

  override def flush(): Unit = {
    if (!closed) pipedOut.flush()
  }

  /**
    * Closes the stream and completes the S3 upload.
    * Blocks until upload is complete. Throws IOException if upload failed.
    */
  override def close(): Unit = {
    if (closed) return

    closed = true
    try {
      pipedOut.close()
      Await.result(uploadFuture, Duration.Inf)
      checkUploadSuccess()
    } catch {
      case e: IOException => throw e
      case e: Exception =>
        S3StorageClient.deleteObject(bucketName, objectKey)
        throw new IOException(s"Failed to complete upload: ${e.getMessage}", e)
    }
  }

  private def whenOpen[T](f: => T): T = {
    if (closed) throw new IOException("Stream is closed")
    checkUploadSuccess()
    f
  }

  private def checkUploadSuccess(): Unit = {
    uploadException.get().foreach { ex =>
      throw new IOException("Background upload failed", ex)
    }
  }
}
