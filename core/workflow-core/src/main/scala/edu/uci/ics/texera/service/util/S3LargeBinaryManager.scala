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

package edu.uci.ics.texera.service.util

import software.amazon.awssdk.services.s3.model.{
  AbortMultipartUploadRequest,
  CompleteMultipartUploadRequest,
  CompletedMultipartUpload,
  CompletedPart,
  CreateMultipartUploadRequest,
  DeleteObjectRequest,
  UploadPartRequest,
  GetObjectRequest
}
import edu.uci.ics.amber.core.storage.StorageConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import software.amazon.awssdk.core.sync.RequestBody
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.S3ReferenceCountsDao
import edu.uci.ics.texera.dao.jooq.generated.tables.{S3ReferenceCounts => S3ReferenceCountsTable}

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
  * A utility class for managing large binary files in S3 storage, including upload, reference counting, and cleanup.
  * This class provides a centralized way to handle large binary file operations in S3 with proper error handling
  * and type safety.
  */
object S3LargeBinaryManager {

  private val BUFFER_SIZE = 1024 * 1024 // 1MB buffer size
  private val MIN_PART_SIZE =
    StorageConfig.s3MultipartUploadPartSize // Use configured part size from StorageConfig
  private val MAX_CONCURRENT_UPLOADS = 10 // Maximum number of concurrent uploads
  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /**
    * Uploads a file to S3 using multipart upload for better performance and reliability.
    * This method implements several performance optimizations:
    * 1. Uses buffered streaming with a 1MB buffer size to reduce memory usage
    * 2. Implements concurrent part uploads (up to 10 concurrent uploads) for better throughput
    * 3. Uses temporary files for part storage to handle large files efficiently
    * 4. Implements automatic cleanup of temporary files and failed uploads
    * 5. Uses multipart upload with 10MB minimum part size as per S3 best practices
    *
    * Performance considerations:
    * - For optimal performance, ensure the input stream is buffered
    * - Larger files will benefit more from the concurrent upload feature
    * - Network bandwidth and latency will be the main bottlenecks
    * - Memory usage is optimized by streaming and temporary file usage
    *
    * @param inputStream The input stream of the file to upload. Should be buffered for better performance.
    * @return The S3 URI of the uploaded file in the format "s3://bucket-name/key"
    * @throws Exception if the upload fails or is interrupted
    */
  def uploadFile(inputStream: InputStream): String = {
    val bucketName = StorageConfig.s3LargeBinaryBucketName
    val key = java.util.UUID.randomUUID().toString
    val bufferedStream = new BufferedInputStream(inputStream, BUFFER_SIZE)
    var uploadId: String = null

    try {
      val createMultipartUploadResponse = S3StorageClient.getS3Client.createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(key).build()
      )
      uploadId = createMultipartUploadResponse.uploadId()

      val completedParts = new ConcurrentHashMap[Int, CompletedPart]()
      val buffer = new Array[Byte](BUFFER_SIZE)
      var currentPartBytes = 0L
      var totalBytes = 0L
      var partNumber = 1

      var currentTempFile = java.io.File.createTempFile("s3-part-", ".tmp")
      var partOutputStream = new java.io.FileOutputStream(currentTempFile)
      var previousTempFile: java.io.File = null
      val uploadFutures = ArrayBuffer[Future[Unit]]()

      try {
        var bytesRead = bufferedStream.read(buffer)
        while (bytesRead != -1) {
          partOutputStream.write(buffer, 0, bytesRead)
          currentPartBytes += bytesRead
          totalBytes += bytesRead

          if (currentPartBytes >= MIN_PART_SIZE) {
            partOutputStream.close()

            previousTempFile = currentTempFile
            currentTempFile = java.io.File.createTempFile("s3-part-", ".tmp")
            partOutputStream = new java.io.FileOutputStream(currentTempFile)

            val currentPartNumber = partNumber
            val currentPartFile = previousTempFile
            val future: Future[Unit] = Future {
              val uploadPartResponse = S3StorageClient.getS3Client.uploadPart(
                UploadPartRequest
                  .builder()
                  .bucket(bucketName)
                  .key(key)
                  .uploadId(uploadId)
                  .partNumber(currentPartNumber)
                  .build(),
                RequestBody.fromFile(currentPartFile)
              )

              completedParts.put(
                currentPartNumber,
                CompletedPart
                  .builder()
                  .partNumber(currentPartNumber)
                  .eTag(uploadPartResponse.eTag())
                  .build()
              )

              if (currentPartFile.exists()) currentPartFile.delete()
              ()
            }
            uploadFutures += future

            if (uploadFutures.size >= MAX_CONCURRENT_UPLOADS) {
              Await.result(Future.sequence(uploadFutures), Duration.Inf)
              uploadFutures.clear()
            }

            partNumber += 1
            currentPartBytes = 0
          }
          bytesRead = bufferedStream.read(buffer)
        }

        if (currentPartBytes > 0) {
          partOutputStream.close()
          val currentPartNumber = partNumber
          val currentPartFile = currentTempFile
          val future: Future[Unit] = Future {
            val uploadPartResponse = S3StorageClient.getS3Client.uploadPart(
              UploadPartRequest
                .builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(currentPartNumber)
                .build(),
              RequestBody.fromFile(currentPartFile)
            )

            completedParts.put(
              currentPartNumber,
              CompletedPart
                .builder()
                .partNumber(currentPartNumber)
                .eTag(uploadPartResponse.eTag())
                .build()
            )

            if (currentPartFile.exists()) currentPartFile.delete()
            ()
          }
          uploadFutures += future
        }

        if (uploadFutures.nonEmpty) {
          Await.result(Future.sequence(uploadFutures), Duration.Inf)
        }

        val sortedParts = (1 to partNumber)
          .flatMap(i => Option(completedParts.get(i)))
          .toList
          .sortBy(_.partNumber())

        S3StorageClient.getS3Client.completeMultipartUpload(
          CompleteMultipartUploadRequest
            .builder()
            .bucket(bucketName)
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(sortedParts.asJava).build())
            .build()
        )

        s"s3://$bucketName/$key"
      } catch {
        case e: Exception =>
          if (uploadId != null) {
            S3StorageClient.getS3Client.abortMultipartUpload(
              AbortMultipartUploadRequest
                .builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .build()
            )
          }
          throw e
      } finally {
        partOutputStream.close()
        if (currentTempFile != null && currentTempFile.exists()) currentTempFile.delete()
        if (previousTempFile != null && previousTempFile.exists()) previousTempFile.delete()
      }
    } finally {
      bufferedStream.close()
    }
  }

  /**
    * Increments the reference count for an S3 object
    *
    * @param s3Uri The S3 URI of the object
    * @throws IllegalArgumentException if the S3 URI format is invalid
    */
  def incrementReferenceCount(s3Uri: String): Unit = {
    require(s3Uri.startsWith("s3://"), "Invalid S3 URI format")

    val dsl = SqlServer.getInstance().createDSLContext()
    SqlServer.withTransaction(dsl) { ctx =>
      val dao = new S3ReferenceCountsDao(ctx.configuration())

      dao
        .fetchOptionalByS3Uri(s3Uri)
        .ifPresentOrElse(
          record => {
            record.setReferenceCount(record.getReferenceCount + 1)
            dao.update(record)
          },
          () => {
            ctx
              .insertInto(S3ReferenceCountsTable.S3_REFERENCE_COUNTS)
              .columns(
                S3ReferenceCountsTable.S3_REFERENCE_COUNTS.S3_URI,
                S3ReferenceCountsTable.S3_REFERENCE_COUNTS.REFERENCE_COUNT
              )
              .values(s3Uri, 1)
              .execute()
          }
        )
    }
  }

  /**
    * Decrements the reference count for an S3 object and deletes it if the count reaches zero
    *
    * @param s3Uri The S3 URI of the object
    * @return The new reference count
    * @throws IllegalArgumentException if the S3 URI format is invalid
    */
  def decrementReferenceCount(s3Uri: String): Long = {
    require(s3Uri.startsWith("s3://"), "Invalid S3 URI format")

    val dsl = SqlServer.getInstance().createDSLContext()
    SqlServer.withTransaction(dsl) { ctx =>
      val dao = new S3ReferenceCountsDao(ctx.configuration())

      val record = dao.fetchOptionalByS3Uri(s3Uri)
      if (record.isPresent) {
        val currentRecord = record.get()
        val newCount = currentRecord.getReferenceCount - 1

        if (newCount <= 0) {
          dao.deleteById(s3Uri)

          val uri = new URI(s3Uri)
          val bucketName = uri.getHost
          val key = uri.getPath.stripPrefix("/")

          S3StorageClient.getS3Client.deleteObject(
            DeleteObjectRequest.builder().bucket(bucketName).key(key).build()
          )
          0
        } else {
          currentRecord.setReferenceCount(newCount)
          dao.update(currentRecord)
          newCount
        }
      } else {
        0
      }
    }
  }

  private def loadReferenceCount(bucketName: String, referenceCountKey: String): Int = {
    try {
      val response = S3StorageClient.getS3Client.getObject(
        GetObjectRequest.builder().bucket(bucketName).key(referenceCountKey).build()
      )
      val bytes = response.readAllBytes()
      if (bytes.isEmpty) 0
      else objectMapper.readValue(bytes, classOf[Int])
    } catch {
      case _: Exception => 0
    }
  }

  /**
    * Gets the size of an S3 object
    * @param s3Uri The S3 URI of the object in format "s3://bucket-name/key"
    * @return The total size of the object in bytes
    * @throws IllegalArgumentException if the S3 URI format is invalid
    */
  def getObjectInfo(s3Uri: String): Long = {
    require(s3Uri.startsWith("s3://"), "Invalid S3 URI format")

    val uri = new URI(s3Uri)
    val bucketName = uri.getHost
    val key = uri.getPath.stripPrefix("/")

    S3StorageClient.getS3Client
      .getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build())
      .response()
      .contentLength()
  }
}
